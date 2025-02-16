// 1. Event System
class EventEmitter {
    constructor() {
        this.listeners = new Map();
    }
    
    on(event, callback) {
        if (!this.listeners.has(event)) {
            this.listeners.set(event, new Set());
        }
        this.listeners.get(event).add(callback);
    }
    
    emit(event, data) {
        if (this.listeners.has(event)) {
            for (const callback of this.listeners.get(event)) {
                callback(data);
            }
        }
    }
}

// 2. Animation Queue
class AnimationQueue {
    constructor() {
        this.queue = [];
        this.isProcessing = false;
    }

    async add(animation) {
        return new Promise((resolve) => {
            this.queue.push({ animation, resolve });
            if (!this.isProcessing) {
                this.process();
            }
        });
    }

    async process() {
        this.isProcessing = true;
        while (this.queue.length > 0) {
            const { animation, resolve } = this.queue.shift();
            await animation();
            resolve();
        }
        this.isProcessing = false;
    }
}

// 3. Core LSM Tree Logic
class Entry {
    constructor(key, value) {
        this.key = key;
        this.value = value;
    }

    toString() {
        return `${this.key}:${this.value}`;
    }

    // Static compare method for sorting
    static compare(a, b) {
        return a.key - b.key;
    }
}

class SSTable {
    constructor(entries) {
        this.data = [...entries].sort(Entry.compare);  // Use the static compare method
        this.minKey = this.data[0].key;
        this.maxKey = this.data[this.data.length - 1].key;
    }

    toString() {
        return `[${this.minKey}-${this.maxKey}]: ${this.data.map(e => e.toString()).join(', ')}`;
    }
}

function compactLevel0(sstables) {
    if (sstables.length === 0) return [];
    
    // Sort SSTables by minKey to make initial grouping easier
    const sorted = [...sstables].sort((a, b) => a.minKey - b.minKey);
    
    // Find all overlapping groups
    const groups = [];
    let currentGroup = [sorted[0]];
    
    // Helper to check if a table overlaps with any table in the group
    const overlapsWithGroup = (sst, group) => {
        return group.some(existing => 
            (sst.minKey <= existing.maxKey && sst.maxKey >= existing.minKey) ||
            (existing.minKey <= sst.maxKey && existing.maxKey >= sst.minKey)
        );
    };
    
    // Group overlapping SSTables
    for (let i = 1; i < sorted.length; i++) {
        const current = sorted[i];
        
        if (overlapsWithGroup(current, currentGroup)) {
            currentGroup.push(current);
        } else {
            // Before starting a new group, check if it overlaps with any existing group
            const existingGroupIndex = groups.findIndex(group => overlapsWithGroup(current, group));
            if (existingGroupIndex !== -1) {
                // Merge current group with existing group
                groups[existingGroupIndex] = [...groups[existingGroupIndex], ...currentGroup, current];
            } else {
                groups.push(currentGroup);
                currentGroup = [current];
            }
        }
    }
    groups.push(currentGroup);
    
    // Merge each group into a single SSTable
    return groups.map(group => {
        // Create a Map to keep only the latest value for each key
        const keyMap = new Map();
        
        // Process SSTables in reverse order (most recent first)
        // This ensures we keep the most recent value for each key
        [...group].reverse().flatMap(sst => sst.data).forEach(entry => {
            if (!keyMap.has(entry.key)) {
                keyMap.set(entry.key, entry);
            }
        });
        
        // Convert map back to sorted array
        const allData = Array.from(keyMap.values()).sort(Entry.compare);
        return new SSTable(allData);
    });
}

function mergeSSTables(thisLevel, nextLevel) {
    if (thisLevel.length === 0) return nextLevel;
    if (nextLevel.length === 0) return thisLevel;
    
    // Find overlapping groups between levels
    const mergedTables = [];
    
    // Helper to check if tables overlap
    const tablesOverlap = (table1, table2) => 
        (table1.minKey <= table2.maxKey && table1.maxKey >= table2.minKey);
    
    // For each table in nextLevel, find all overlapping tables from thisLevel
    nextLevel.forEach(nextTable => {
        const overlappingTables = thisLevel.filter(thisTable => 
            tablesOverlap(thisTable, nextTable)
        );
        
        if (overlappingTables.length === 0) {
            // No overlaps, keep nextTable as is
            mergedTables.push(nextTable);
        } else {
            // Merge overlapping tables
            const keyMap = new Map();
            
            // First add all entries from nextTable
            nextTable.data.forEach(entry => {
                keyMap.set(entry.key, entry);
            });
            
            // Then add entries from thisLevel tables (overwriting any duplicates)
            overlappingTables.forEach(thisTable => {
                thisTable.data.forEach(entry => {
                    keyMap.set(entry.key, entry);
                });
            });
            
            // Create new SSTable with merged data
            const mergedData = Array.from(keyMap.values()).sort(Entry.compare);
            mergedTables.push(new SSTable(mergedData));
        }
    });
    
    // Add any tables from thisLevel that don't overlap with anything in nextLevel
    thisLevel.forEach(thisTable => {
        if (!nextLevel.some(nextTable => tablesOverlap(thisTable, nextTable))) {
            mergedTables.push(thisTable);
        }
    });
    
    // Sort final tables by minKey
    return mergedTables.sort((a, b) => a.minKey - b.minKey);
}

class LSMTree {
    constructor(config) {
        this.config = config;
        this.memtable = [];  // Will hold Entry objects
        this.levels = [[], []];  
        this.events = new EventEmitter();
        this.busy = false;
    }

    printState() {
        console.log('\n=== LSM Tree State ===');
        console.log('Memtable:', this.memtable.map(e => e.toString()).join(', ') || '(empty)');
        this.levels.forEach((level, index) => {
            console.log(`Disk Level ${index}:`, 
                level.length ? level.map(sst => sst.toString()).join(' | ') : '(empty)');
        });
        console.log('==================\n');
    }

    async insert(key, value) {
        if (this.busy) return;
        
        if (this.config.print) {
            console.log(`\nInserting ${key}:${value}`);
        }
        this.events.emit('beforeInsert', { key, value, level: 'memtable' });
        
        if (this.memtable.length === this.config.maxMemtableSize) {
            await this.flushMemtable();
        }
        
        this.memtable.push(new Entry(key, value));
        this.memtable.sort(Entry.compare);  // Use the static compare method
        
        this.events.emit('afterInsert', { 
            key,
            value,
            level: 'memtable',
            levelState: this.memtable
        });

        if (this.config.print) {
            this.printState();
        }
    }

    async flushMemtable() {
        if (this.busy) return;
        this.busy = true;

        if (this.config.print) {
            console.log('\nFlushing memtable to disk level 0...');
        }

        try {
            const newSSTable = new SSTable(this.memtable);
            
            this.events.emit('flushStart', {
                sourceLevel: 'memtable',
                sourceLevelState: [...this.memtable],
                targetLevel: 0,
                targetLevelState: this.levels[0].map(sst => sst.data).flat()
            });

            if (this.levels[0].length === this.config.maxElementsPerLevel[0]) {
                await this.flush(0);
            }
            this.levels[0].push(newSSTable);
            
            this.memtable = [];

            this.events.emit('flushComplete', {
                sourceLevel: 'memtable',
                targetLevel: 0,
                newState: this.getSnapshot()
            });

            if (this.config.print) {
                this.printState();
            }
        } finally {
            this.busy = false;
        }
    }

    async flush(level) {
        this.events.emit('flushStart', {
            sourceLevel: level,
            sourceLevelState: [...this.levels[level]],
            targetLevel: level + 1,
            targetLevelState: [...this.levels[level + 1]]
        });

        if (level === 0) {
            this.levels[level] = compactLevel0(this.levels[level]);
        }

        const mergedSSTables = mergeSSTables(this.levels[level], this.levels[level + 1]);

        this.events.emit('mergeComplete', {
            sourceLevel: level,
            targetLevel: level + 1,
            mergedElements: [...mergedSSTables]
        });

        // Update state
        this.levels[level] = [];
        this.levels[level + 1] = mergedSSTables;

        this.events.emit('flushComplete', {
            sourceLevel: level,
            targetLevel: level + 1,
            newState: this.getSnapshot()
        });

        if (
            level + 1 < this.levels.length - 1 && 
            this.levels[level + 1].length > this.config.maxElementsPerLevel[level + 1]
        ) {
            await this.flush(level + 1);
        }

        if (this.config.print) {
            this.printState();
        }
    }

    getSnapshot() {
        return {
            memtable: [...this.memtable],
            levels: this.levels.map(level => 
                level.map(sst => sst.data).flat()  // Flatten SSTables for visualization
            ),
            timestamp: Date.now()
        };
    }
}

// 4. Visualization Manager
class LSMTreeVisualizer {
    constructor(tree, svgElement, config) {
        this.tree = tree;
        this.svg = d3.select(svgElement);
        this.config = config;
        this.animationQueue = new AnimationQueue();
        this.setupSubscriptions();
        this.setupLayout();
    }

    setupSubscriptions() {
        this.tree.events.on('beforeInsert', this.handleBeforeInsert.bind(this));
        this.tree.events.on('afterInsert', this.handleAfterInsert.bind(this));
        this.tree.events.on('flushStart', this.handleFlushStart.bind(this));
        this.tree.events.on('mergeComplete', this.handleMergeComplete.bind(this));
        this.tree.events.on('flushComplete', this.handleFlushComplete.bind(this));
    }

    setupLayout() {
        const width = parseInt(this.svg.style("width"));
        const margin = { top: 20, right: 20, bottom: 20, left: 60 };

        // Add level labels
        this.svg.selectAll(".level-label")
            .data(["Memory Buffer", "Disk Level 1", "Disk Level 2"])
            .enter()
            .append("text")
            .attr("class", "level-label")
            .attr("x", margin.left - 10)
            .attr("y", (d, i) => margin.top + (i * this.config.levelHeight) + this.config.elementSize)
            .attr("text-anchor", "end")
            .text(d => d);
    }

    async handleBeforeInsert(data) {
        // Could add pre-insertion animations here
    }

    async handleAfterInsert(data) {
        await this.animationQueue.add(async () => {
            await this.updateLevel(data.level, data.levelState);
        });
    }

    async handleFlushStart(data) {
        await this.animationQueue.add(async () => {
            // Remove merging class from all elements first
            this.svg.selectAll(".merging").classed("merging", false);
            
            // Highlight source level
            await this.highlightLevelForMerge(data.sourceLevel);
            // Also highlight target level
            await this.highlightLevelForMerge(data.targetLevel);
        });
    }

    async handleMergeComplete(data) {
        await this.animationQueue.add(async () => {
            // Calculate vertical positions
            const sourceY = this.config.margin.top + 
                ((data.sourceLevel === 'memtable' ? 0 : data.sourceLevel) * this.config.levelHeight);
            const targetY = this.config.margin.top + 
                (data.targetLevel * this.config.levelHeight);
            const midY = (sourceY + targetY) / 2;

            // Create merged view at source level position
            const mergedGroup = this.svg.append("g")
                .attr("class", "merged-group")
                .attr("transform", `translate(0, ${sourceY})`);

            // Add merged elements
            const elements = mergedGroup.selectAll(".merged-element")
                .data(data.mergedElements)
                .join("g")
                .attr("class", "merged-element")
                .attr("transform", (d, i) => {
                    const x = this.config.margin.left + (i * (this.config.elementSize + 10));
                    return `translate(${x}, 0)`;
                });

            elements.append("circle")
                .attr("r", this.config.elementSize / 2)
                .attr("class", "merging");

            elements.append("text")
                .attr("text-anchor", "middle")
                .attr("dy", "0.3em")
                .text(d => d);

            // Animate to middle position
            await new Promise(resolve => {
                mergedGroup.transition()
                    .duration(500)
                    .attr("transform", `translate(0, ${midY})`)
                    .on("end", resolve);
            });

            // Pause briefly
            await new Promise(resolve => setTimeout(resolve, 250));

            // Animate to target position
            await new Promise(resolve => {
                mergedGroup.transition()
                    .duration(500)
                    .attr("transform", `translate(0, ${targetY})`)
                    .on("end", resolve);
            });

            // Pause briefly before cleanup
            await new Promise(resolve => setTimeout(resolve, 250));

            // Clean up
            mergedGroup.remove();
            this.svg.selectAll(".merging").classed("merging", false);
        });
    }

    async handleFlushComplete(data) {
        await this.animationQueue.add(async () => {
            await this.updateFromSnapshot(data.newState);
        });
    }

    async updateFromSnapshot(snapshot) {
        // Clear any remaining merge animations
        this.svg.selectAll(".merging").classed("merging", false);
        this.svg.selectAll(".merged-group").remove();

        // Update memtable first
        await this.updateLevel('memtable', snapshot.memtable);
        // Then update disk levels
        for (let i = 0; i < snapshot.levels.length; i++) {
            await this.updateLevel(i, snapshot.levels[i]);
        }
    }

    async updateLevel(levelIndex, levelState) {
        // Convert 'memtable' to 0 for positioning
        const yPosition = levelIndex === 'memtable' ? 0 : levelIndex;
        
        // First, ensure we remove any existing elements for this level
        this.svg.selectAll(`.level-group-${levelIndex}`).remove();
        
        const levelGroup = this.svg.append("g")
            .attr("class", `level-group-${levelIndex}`)
            .attr("transform", `translate(0, ${this.config.margin.top + (yPosition * this.config.levelHeight)})`);

        const elements = levelGroup.selectAll(`.level-${levelIndex}`)
            .data(levelState)
            .enter()
            .append("g")
            .attr("class", `element level-${levelIndex}`)
            .attr("transform", (d, i) => {
                const x = this.config.margin.left + (i * (this.config.elementSize + 10));
                return `translate(${x}, 0)`;
            });

        // Add circles with transition
        elements.append("circle")
            .attr("r", 0)  // Start with radius 0
            .attr("class", d => levelIndex === 'memtable' ? "memory-buffer" : "disk-level")
            .transition()
            .duration(500)
            .attr("r", this.config.elementSize / 2);  // Animate to full size

        // Add text
        elements.append("text")
            .attr("text-anchor", "middle")
            .attr("dy", "0.3em")
            .text(d => d)
            .style("opacity", 0)  // Start invisible
            .transition()
            .duration(500)
            .style("opacity", 1);  // Fade in

        return new Promise(resolve => setTimeout(resolve, 500));
    }

    async highlightLevelForMerge(levelIndex) {
        // Handle both memtable and numeric level indices
        this.svg.selectAll(`.level-${levelIndex} circle`)
            .classed("merging", true);
        return new Promise(resolve => setTimeout(resolve, 500));
    }
}

// 5. Configuration and Setup
const config = {
    maxMemtableSize: 4,
    maxElementsPerLevel: [4, 8, 16],
    elementSize: 40,
    levelHeight: 120,
    margin: { top: 20, right: 20, bottom: 20, left: 60 },
    print: true,
};

// Initialize
const lsmTree = new LSMTree(config);
const visualizer = new LSMTreeVisualizer(lsmTree, "#lsm-svg", config);

// Wire up UI controls
const demoSequence = [
    // First batch - first range (10-20)
    { key: 10, value: 'a' },
    { key: 15, value: 'b' },
    { key: 20, value: 'c' },
    
    // Second batch - second range (40-60)
    { key: 40, value: 'd' },
    { key: 50, value: 'e' },
    { key: 60, value: 'f' },
    
    // Third batch - third range (80-95)
    { key: 80, value: 'g' },
    { key: 85, value: 'h' },
    { key: 95, value: 'i' },
    
    // Fourth batch - overlaps with first range, updates values
    { key: 12, value: 'j' },
    { key: 15, value: 'k' },  // overwrites 15:b
    { key: 18, value: 'l' },
    
    // Fifth batch - fills some gaps
    { key: 30, value: 'm' },  // between ranges
    { key: 70, value: 'n' },  // between ranges
    { key: 90, value: 'o' },  // in third range
    
    // Sixth batch - more gap filling
    { key: 45, value: 'p' },  // in second range
    { key: 55, value: 'q' },  // in second range
    { key: 75, value: 'r' },  // between ranges
    
    // Seventh batch - final updates
    { key: 15, value: 's' },  // overwrites 15:k
    { key: 50, value: 't' },  // overwrites 50:e
    { key: 85, value: 'u' },  // overwrites 85:h
    
    // Eighth batch - last insertions
    { key: 25, value: 'v' },  // between ranges
    { key: 65, value: 'w' },  // between ranges
    { key: 92, value: 'x' },  // in third range
    
    // Ninth batch - very last updates
    { key: 18, value: 'y' },  // overwrites 18:l
    { key: 75, value: 'z' },  // overwrites 75:r
    { key: 92, value: 'a' }   // overwrites 92:x
];

let currentIndex = 0;
document.getElementById("insertBtn").addEventListener("click", () => {
    if (currentIndex < demoSequence.length) {
        const { key, value } = demoSequence[currentIndex++];
        console.log(`Inserting ${key}:${value} (${currentIndex}/${demoSequence.length})`);
        lsmTree.insert(key, value);
    } else {
        console.log("Demo sequence complete!");
    }
});

document.getElementById("flushBtn").addEventListener("click", () => {
    lsmTree.flushMemtable();
});

// Test mergeSSTables function
function testCompactLevel0() {
    console.log("=== Testing SSTable Merging ===");
    
    // Create test SSTables
    const tables = [
        new SSTable([new Entry(1, 'a'), new Entry(2, 'b'), new Entry(3, 'c')]),
        new SSTable([new Entry(2, 'b'), new Entry(3, 'c'), new Entry(4, 'd')]),
        new SSTable([new Entry(3, 'c'), new Entry(4, 'd'), new Entry(5, 'e')]),
        new SSTable([new Entry(7, 'f'), new Entry(8, 'g'), new Entry(9, 'h')])
    ];
    
    console.log("Input SSTables:");
    tables.forEach(sst => console.log(sst.toString()));
    
    const merged = compactLevel0(tables);
    
    console.log("\nMerged SSTables:");
    merged.forEach(sst => console.log(sst.toString()));
    
    // Verify results
    const expected = [
        new SSTable([
            new Entry(1, 'a'), new Entry(2, 'b'), new Entry(3, 'c'),
            new Entry(4, 'd'), new Entry(5, 'e')
        ]),
        new SSTable([new Entry(7, 'f'), new Entry(8, 'g'), new Entry(9, 'h')])
    ];
    
    const correct = merged.length === expected.length &&
        merged.every((sst, i) => 
            sst.minKey === expected[i].minKey &&
            sst.maxKey === expected[i].maxKey &&
            JSON.stringify(sst.data) === JSON.stringify(expected[i].data)
        );
    
    console.log("\nTest result:", correct ? "PASSED" : "FAILED");
    console.log("==================\n");
}

function testCompactLevel02() {
    console.log("=== Testing SSTable Merging ===");
    
    // Create test SSTables
    const tables = [
        new SSTable([new Entry(44, 'a'), new Entry(84, 'b')]),
        new SSTable([new Entry(0, 'c')]),
        new SSTable([new Entry(88, 'd')]),
        new SSTable([new Entry(10, 'e')]),
        new SSTable([
            new Entry(0, 'c'), new Entry(26, 'f'), new Entry(42, 'g'),
            new Entry(44, 'a'), new Entry(46, 'h'), new Entry(54, 'i'),
            new Entry(59, 'j'), new Entry(65, 'k'), new Entry(83, 'l'),
            new Entry(96, 'm')
        ])
    ];
    
    console.log("Input SSTables:");
    tables.forEach(sst => console.log(sst.toString()));
    
    const merged = compactLevel0(tables);
    
    console.log("\nMerged SSTables:");
    merged.forEach(sst => console.log(sst.toString()));
    
    // Verify results
    const expected = [
        new SSTable([
            new Entry(0, 'c'), new Entry(10, 'e'), new Entry(26, 'f'),
            new Entry(42, 'g'), new Entry(44, 'a'), new Entry(46, 'h'),
            new Entry(54, 'i'), new Entry(59, 'j'), new Entry(65, 'k'),
            new Entry(83, 'l'), new Entry(84, 'b'), new Entry(88, 'd'),
            new Entry(96, 'm')
        ])
    ];
    
    const correct = merged.length === expected.length &&
        merged.every((sst, i) => 
            sst.minKey === expected[i].minKey &&
            sst.maxKey === expected[i].maxKey &&
            JSON.stringify(sst.data) === JSON.stringify(expected[i].data)
        );
    
    console.log("\nTest result:", correct ? "PASSED" : "FAILED");
    console.log("==================\n");
}

function testCompactLevel0Simple() {
    console.log("=== Testing Level 0 Compaction (Simple) ===");
    
    // Create test SSTables in order of insertion (oldest to newest)
    const tables = [
        new SSTable([new Entry(1, 'a'), new Entry(2, 'b')]),
        new SSTable([new Entry(2, 'c'), new Entry(3, 'd')]),
        new SSTable([new Entry(1, 'b')])  
    ];
    
    console.log("Input SSTables (oldest to newest):");
    tables.forEach(sst => console.log(sst.toString()));
    
    const compacted = compactLevel0(tables);
    
    console.log("\nCompacted SSTables:");
    compacted.forEach(sst => console.log(sst.toString()));
    
    // Verify results - should keep newest values
    const expected = [
        new SSTable([
            new Entry(1, 'b'),  // newest value for key 1
            new Entry(2, 'c'),  // newest value for key 2
            new Entry(3, 'd')   // newest value for key 3
        ])
    ];
    
    const correct = compacted.length === expected.length &&
        compacted.every((sst, i) => 
            sst.minKey === expected[i].minKey &&
            sst.maxKey === expected[i].maxKey &&
            JSON.stringify(sst.data) === JSON.stringify(expected[i].data)
        );
    
    console.log("\nTest result:", correct ? "PASSED" : "FAILED");
    console.log("==================\n");
}

async function testLSMTreeLevel0Compaction() {
    console.log("=== Testing LSM Tree Level 0 Compaction ===");
    
    // Create LSM tree with small sizes to force compaction
    const testConfig = {
        maxMemtableSize: 2,  // Force frequent memtable flushes
        maxElementsPerLevel: [2, 2, 2],  // Force level 0 compaction
        elementSize: 40,
        levelHeight: 120,
        margin: { top: 20, right: 20, bottom: 20, left: 60 },
        print: false
    };
    
    const tree = new LSMTree(testConfig);
    
    // Insert sequence that will force compaction
    await tree.insert(1, 'a');  
    await tree.insert(2, 'b');  
    await tree.insert(1, 'c');  
    await tree.insert(3, 'd');  
    await tree.insert(5, 'f');  
    await tree.insert(6, 'g');  
    await tree.insert(7, 'h');  
    
    console.log("\nFinal state:");
    tree.printState();
    
    // Check if level 1 exists and has data
    if (!tree.levels[1] || !tree.levels[1][0]) {
        console.log("\nTest result: FAILED - Level 1 is empty");
        console.log("==================\n");
        return;
    }
    
    // Verify level 1 has the correct values (most recent wins)
    const level1Data = tree.levels[1][0].data;
    const expected = [
        new Entry(1, 'c'),  // Should have 'c' not 'a'
        new Entry(2, 'b'),
        new Entry(3, 'd')
    ];
    
    const correct = level1Data.length === expected.length &&
        level1Data.every((entry, i) => 
            entry.key === expected[i].key &&
            entry.value === expected[i].value
        );
    
    console.log("\nExpected level 1 data:", expected.map(e => e.toString()).join(', '));
    console.log("Actual level 1 data:", level1Data.map(e => e.toString()).join(', '));
    console.log("\nTest result:", correct ? "PASSED" : "FAILED");
    console.log("==================\n");
}

async function testLSMTreeMergeCompaction() {
    console.log("=== Testing LSM Tree Merge Compaction ===");
    
    // Create LSM tree with small sizes to force compaction
    const testConfig = {
        maxMemtableSize: 2,  // Force frequent memtable flushes
        maxElementsPerLevel: [2, 2, 2],  // Force level 0 compaction
        elementSize: 40,
        levelHeight: 120,
        margin: { top: 20, right: 20, bottom: 20, left: 60 },
        print: false
    };
    
    const tree = new LSMTree(testConfig);
    
    // Insert sequence that will force compaction
    await tree.insert(1, 'a');  
    await tree.insert(2, 'b');  
    await tree.insert(1, 'c');  
    await tree.insert(3, 'd');  
    await tree.insert(5, 'f');  
    await tree.insert(1, 'g');  

    await tree.insert(7, 'h');  
    await tree.insert(8, 'i');  
    await tree.insert(9, 'j');  
    await tree.insert(10, 'k');  
    await tree.insert(11, 'l');  
    
    console.log("\nFinal state:");
    tree.printState();
    
    // Check if level 1 exists and has data
    if (!tree.levels[1] || !tree.levels[1][0]) {
        console.log("\nTest result: FAILED - Level 1 is empty");
        console.log("==================\n");
        return;
    }
    
    // Verify level 1 has the correct values (most recent wins)
    const level1Data = tree.levels[1];
    const expected = [
        new SSTable([
            new Entry(1, 'g'),  
            new Entry(2, 'b'),
            new Entry(3, 'd'),
            new Entry(5, 'f'),
        ]),
        new SSTable([
            new Entry(7, 'h'),
            new Entry(8, 'i'),
        ])
    ];
    
    let correct = true;
    for (let i = 0; i < level1Data.length; i++) {
        for (let j = 0; j < level1Data[i].data.length; j++) {
            if (level1Data[i].data[j].key !== expected[i].data[j].key || level1Data[i].data[j].value !== expected[i].data[j].value) {
                correct = false;
                break;
            }
        }
    }
    
    console.log("\nExpected level 1 data:", expected.map(e => e.toString()).join(', '));
    console.log("Actual level 1 data:", level1Data.map(e => e.toString()).join(', '));
    console.log("\nTest result:", correct ? "PASSED" : "FAILED");
    console.log("==================\n");
}

async function testLSMTreeLevelMerge() {
    console.log("=== Testing LSM Tree Level Merge ===");
    
    // Create test data
    const level0 = [
        new SSTable([new Entry(1, 'newer'), new Entry(2, 'newer')]),
        new SSTable([new Entry(5, 'newer')])
    ];
    
    const level1 = [
        new SSTable([new Entry(1, 'old'), new Entry(2, 'old'), new Entry(3, 'old')]),
        new SSTable([new Entry(6, 'old'), new Entry(7, 'old')])
    ];
    
    console.log("Level 0 (newer):");
    level0.forEach(sst => console.log(sst.toString()));
    console.log("\nLevel 1 (older):");
    level1.forEach(sst => console.log(sst.toString()));
    
    const merged = mergeSSTables(level0, level1);
    
    console.log("\nMerged result:");
    merged.forEach(sst => console.log(sst.toString()));
    
    // Verify results
    const expected = [
        new SSTable([
            new Entry(1, 'newer'),  // from level 0
            new Entry(2, 'newer'),  // from level 0
            new Entry(3, 'old')     // from level 1 (no overlap)
        ]),
        new SSTable([new Entry(5, 'newer')]),  // from level 0
        new SSTable([new Entry(6, 'old'), new Entry(7, 'old')])  // from level 1 (no overlap)
    ];
    
    const correct = merged.length === expected.length &&
        merged.every((sst, i) => 
            sst.minKey === expected[i].minKey &&
            sst.maxKey === expected[i].maxKey &&
            JSON.stringify(sst.data) === JSON.stringify(expected[i].data)
        );
    
    console.log("\nTest result:", correct ? "PASSED" : "FAILED");
    console.log("==================\n");
}

// Run all tests
(async () => {
    testCompactLevel0();
    testCompactLevel02();
    testCompactLevel0Simple();
    await testLSMTreeLevel0Compaction();
    await testLSMTreeMergeCompaction();
    await testLSMTreeLevelMerge();
})();