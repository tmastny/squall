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
    
}

function mergeSSTables(sstables) {
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
        group.flatMap(sst => sst.data).forEach(entry => {
            keyMap.set(entry.key, entry);
        });
        
        // Convert map back to sorted array
        const allData = Array.from(keyMap.values()).sort(Entry.compare);
        return new SSTable(allData);
    });
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
        
        console.log(`\nInserting ${key}:${value}`);
        this.events.emit('beforeInsert', { key, value, level: 'memtable' });
        
        if (this.memtable.length === this.config.maxMemtableSize) {
            console.log('Memtable full, triggering flush...');
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

        this.printState();
    }

    async flushMemtable() {
        if (this.busy) return;
        this.busy = true;

        try {
            console.log('\nFlushing memtable to disk level 0...');
            
            // Create new SSTable from memtable
            const newSSTable = new SSTable(this.memtable);
            
            this.events.emit('flushStart', {
                sourceLevel: 'memtable',
                sourceLevelState: [...this.memtable],
                targetLevel: 0,
                targetLevelState: this.levels[0].map(sst => sst.data).flat()
            });

            console.log(this.levels[0].length, this.config.maxElementsPerLevel[0]);
            if (this.levels[0].length === this.config.maxElementsPerLevel[0]) {
                await this.flush(0);
            }
            this.levels[0].push(newSSTable);
            
            // Clear memtable
            this.memtable = [];

            this.events.emit('flushComplete', {
                sourceLevel: 'memtable',
                targetLevel: 0,
                newState: this.getSnapshot()
            });

            this.printState();
        } finally {
            this.busy = false;
        }
    }

    async flush(level) {
        console.log(`\nFlushing disk level ${level} to level ${level + 1}...`);
        this.events.emit('flushStart', {
            sourceLevel: level,
            sourceLevelState: [...this.levels[level]],
            targetLevel: level + 1,
            targetLevelState: [...this.levels[level + 1]]
        });

        const mergedSSTables = mergeSSTables([...this.levels[level], ...this.levels[level + 1]]);

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

        // Check if next level needs flushing
        if (
            level + 1 < this.levels.length - 1 && 
            this.levels[level + 1].length > this.config.maxElementsPerLevel[level + 1]
        ) {
            await this.flush(level + 1);
        }

        this.printState();
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
    margin: { top: 20, right: 20, bottom: 20, left: 60 }
};

// Initialize
const lsmTree = new LSMTree(config);
const visualizer = new LSMTreeVisualizer(lsmTree, "#lsm-svg", config);

// Wire up UI controls
document.getElementById("insertBtn").addEventListener("click", () => {
    const key = Math.floor(Math.random() * 100);
    const value = Math.floor(Math.random() * 100);
    lsmTree.insert(key, value);
});

document.getElementById("flushBtn").addEventListener("click", () => {
    lsmTree.flushMemtable();
});

// Test mergeSSTables function
function testMergeSSTables() {
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
    
    const merged = mergeSSTables(tables);
    
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

function testMergeSSTables2() {
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
    
    const merged = mergeSSTables(tables);
    
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

// Run the test
testMergeSSTables();
testMergeSSTables2();