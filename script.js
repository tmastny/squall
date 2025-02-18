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
        // Set min/max keys only if there is data
        if (this.data.length > 0) {
            this.minKey = this.data[0].key;
            this.maxKey = this.data[this.data.length - 1].key;
        } else {
            // For empty tables, set min/max to null or infinity based on what makes sense
            this.minKey = Infinity;  // Nothing can overlap with an empty table
            this.maxKey = -Infinity; // Nothing can overlap with an empty table
        }
    }

    toString() {
        return `[${this.minKey}-${this.maxKey}]: ${this.data.map(e => e.toString()).join(', ')}`;
    }
}

function mergeOrderedEntries(leftTable, rightTable, emitEvent) {
    const merged = [];
    let leftIdx = 0;  // leftTable (older, from level 1)
    let rightIdx = 0;  // rightTable (newer, from level 0)

    // Emit setup event before starting merge
    emitEvent('setupMergeZone', {
        leftTable,
        rightTable,
        targetLength: leftTable.data.length + rightTable.data.length
    });

    while (leftIdx < leftTable.data.length || rightIdx < rightTable.data.length) {
        // If we've exhausted the newer table, or older key is smaller
        if (rightIdx >= rightTable.data.length ||
            (leftIdx < leftTable.data.length &&
                leftTable.data[leftIdx].key < rightTable.data[rightIdx].key)) {
            emitEvent('mergeStep', {
                type: 'takeLeft',
                leftEntryIndex: leftIdx,
                rightEntryIndex: rightIdx,
                mergedSoFar: [...merged]
            });
            merged.push(leftTable.data[leftIdx]);
            leftIdx++;
        }
        // If we've exhausted the older table, or newer key is smaller/equal
        else if (leftIdx >= leftTable.data.length ||
            rightTable.data[rightIdx].key <= leftTable.data[leftIdx].key) {
            if (leftIdx < leftTable.data.length &&
                rightTable.data[rightIdx].key === leftTable.data[leftIdx].key) {
                emitEvent('mergeStep', {
                    type: 'takeRightOverwrite',
                    leftEntryIndex: leftIdx,
                    rightEntryIndex: rightIdx,
                    mergedSoFar: [...merged]
                });
            } else {
                emitEvent('mergeStep', {
                    type: 'takeRight',
                    leftEntryIndex: leftIdx,
                    rightEntryIndex: rightIdx,
                    mergedSoFar: [...merged]
                });
            }
            merged.push(rightTable.data[rightIdx]);
            // Skip any duplicate keys in older table
            while (leftIdx < leftTable.data.length &&
                leftTable.data[leftIdx].key === rightTable.data[rightIdx].key) {
                leftIdx++;
            }
            rightIdx++;
        }
    }

    // Emit cleanup event after merge is complete
    emitEvent('cleanupMergeZone', {
        mergedEntries: merged,
        finalPosition: 'level1'  // Indicate where the merged result should go
    });

    return merged;
}

function mergeSSTables(thisLevel, nextLevel, events = null) {
    if (thisLevel.length === 0) return nextLevel;

    const emitEvent = (eventName, data) => {
        if (events) {
            events.emit(eventName, data);
        }
    };

    let resultLevel = [...nextLevel];  // Work with a copy of nextLevel

    // Helper to check if tables overlap
    const tablesOverlap = (table1, table2) =>
        (table1.minKey <= table2.maxKey && table1.maxKey >= table2.minKey);

    // Process each table from thisLevel
    thisLevel.forEach((thisTable, i) => {
        // Find all overlapping tables from resultLevel
        const overlappingTables = resultLevel.filter(nextTable =>
            tablesOverlap(thisTable, nextTable)
        );

        // If no overlapping tables, use an empty table
        const tablesToMerge = overlappingTables.length > 0 ? overlappingTables : [new SSTable([])];

        emitEvent('mergeGroupFound', {
            sourceTable: {
                table: thisTable,
                level: 0,  // thisLevel is always level 0
                index: i   // from the forEach index
            },
            overlappingTables: tablesToMerge.map((table, idx) => ({
                table: table,
                level: 1,  // nextLevel is always level 1
                index: resultLevel.indexOf(table)  // get original index in resultLevel
            })),
            allTablesInLevel: {
                level0: thisLevel.length,
                level1: resultLevel.length
            }
        });

        let result = thisTable;

        tablesToMerge.forEach(nextTable => {

            const merged = mergeOrderedEntries(nextTable, result, emitEvent);
            result = new SSTable(merged);
        });

        // Remove the overlapped tables (if any)
        if (overlappingTables.length > 0) {
            resultLevel = resultLevel.filter(table => !overlappingTables.includes(table));
        }

        // Insert result in order by minKey
        const insertIndex = resultLevel.findIndex(table => table.minKey > result.minKey);
        if (insertIndex === -1) {
            resultLevel.push(result);
        } else {
            resultLevel.splice(insertIndex, 0, result);
        }

        // Emit event after insertion with the final state
        emitEvent('mergeComplete', {
            mergedTable: result,
            sourceLevel: 0,
            sourceIndex: i,  // from the forEach index
            targetLevel: 1,
            finalIndex: insertIndex === -1 ? resultLevel.length - 1 : insertIndex,
            targetLevelState: resultLevel  // now includes the newly inserted table
        });
    });

    return resultLevel;
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
        this.events.emit('beforeMemtableInsert', { key, value });

        if (this.memtable.length === this.config.maxMemtableSize) {
            await this.flushMemtable();
        }

        this.memtable.push(new Entry(key, value));
        this.memtable.sort(Entry.compare);  // Use the static compare method

        this.events.emit('afterMemtableInsert', {
            memtable: this.memtable.slice()
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

            this.events.emit('memtableFlushed', {
                memtableState: this.memtable,
                level0State: this.levels[0]
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


        const mergedSSTables = mergeSSTables(this.levels[level], this.levels[level + 1], this.events);

        this.levels[level] = [];
        this.levels[level + 1] = mergedSSTables;

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
        this.tree.events.on('beforeMemtableInsert', this.handleBeforeMemtableInsert.bind(this));
        this.tree.events.on('afterMemtableInsert', this.handleAfterMemtableInsert.bind(this));
        this.tree.events.on('flushStart', this.handleFlushStart.bind(this));
        this.tree.events.on('mergeComplete', this.handleMergeComplete.bind(this));
        this.tree.events.on('flushComplete', this.handleFlushComplete.bind(this));
        this.tree.events.on('memtableFlushed', this.handleMemtableFlushed.bind(this));
        this.tree.events.on('mergeGroupFound', this.handleMergeGroupFound.bind(this));
        this.tree.events.on('mergeStep', this.handleMergeStep.bind(this));
        this.tree.events.on('setupMergeZone', this.handleSetupMergeZone.bind(this));
        this.tree.events.on('cleanupMergeZone', this.handleCleanupMergeZone.bind(this));
    }

    setupLayout() {
        const width = parseInt(this.svg.style("width"));
        const margin = { top: 20, right: 20, bottom: 20, left: 20 };
        const labelHeight = 12; // reduced to 12 (from 25)

        // // Add level labels
        // this.svg.selectAll(".level-label")
        //     .data(["Memtable", "Level 0", "Level 1"])
        //     .enter()
        //     .append("text")
        //     .attr("class", "level-label")
        //     .attr("x", margin.left)  // align with left margin
        //     .attr("y", (d, i) => margin.top + (i * this.config.levelHeight))  // position at top of each section
        //     .attr("text-anchor", "start")
        //     .text(d => d);

        // Update element positioning to start below labels
        this.config.margin = {
            top: margin.top + labelHeight, // offset elements below label
            left: margin.left,
            right: margin.right,
            bottom: margin.bottom
        };
        this.config.levelHeight = 45; // total height including label and elements


        this.memtableData = [];

        // Create the memtable group (the "layer")
        this.memtableGroup = this.svg.append("g")
            .attr("class", "level-group-memtable")
            .attr("transform", `translate(${this.config.margin.left}, ${this.config.margin.top})`);


        this.level0Data = [];
        this.level0Group = this.svg.append("g")
            .attr("class", "level-group-level0")
            .attr("transform", "translate(0, 140)");

        // 1. Create the SVG
        // this.svg = d3.select(config.container)
        //     .append("svg")
        //     .attr("width", config.width)
        //     .attr("height", config.height);

        // 2. The unified data array of all entries
        //    Each item is { key, value, stage: 'memtable' | 'level0' | 'mergeZone' ... }
        this.allEntries = [];
        this.level0Sstables = [];
        this.level0SstableGroup = this.svg.append("g")
            .attr("class", "sstable-level0-layer");
    }

    computePosition(d, memtableItems, level0Items) {
        // Hard-coded row positions:
        const memtableY = 50;
        const level0Y = 140;
        const entrySpacing = 10;
        const elementSize = this.config.elementSize;

        if (d.stage === 'memtable') {
            // find index in memtableItems
            const indexInStage = memtableItems.findIndex(x => x.key === d.key);
            return { x: 50 + indexInStage * (elementSize + entrySpacing), y: memtableY };
        }
        else if (d.stage === 'level0') {
            // 1. Figure out which sstable this entry belongs to
            const sstableId = d.sstableId || 0; // fallback if undefined

            // 2. Among *all level0 items*, find how many items are in the same sstable
            //    sorted by key or insertion order
            const itemsInThisSstable = level0Items.filter(x => x.sstableId === sstableId);

            // find index among those items
            const indexInSstable = itemsInThisSstable.findIndex(x => x.key === d.key);

            // 3. The sstable group might be horizontally spaced by sstableId
            const sstableSpacing = 150;  // must match what you used in updateSstables
            const baseX = 50 + sstableId * sstableSpacing;
            const x = baseX + 10 + indexInSstable * (elementSize + entrySpacing);

            return { x, y: level0Y + 15 };
            // y + 15 if you want items inside the box, 
            // or match how you sized the bounding rect (e.g. height=50).
        }

        // handle other stages if needed...
        return { x: 0, y: 0 };
    }

    updateSstables() {
        // For example, each bounding box in level0 is placed in a row at y=140
        const sstableSpacing = 150;   // how far apart each SStable is horizontally
        const margin = 20; // or from this.config.margin.left, etc.

        const selection = this.svg.selectAll(".sstable-group-level0")
            .data(this.level0Sstables, d => d.id);

        // EXIT
        selection.exit().remove();

        // ENTER
        const enter = selection.enter()
            .append("g")
            .attr("class", "sstable-group-level0")
            .attr("transform", (d, i) => `translate(${margin + i * sstableSpacing}, 140)`);

        // bounding box rect
        enter.append("rect")
            .attr("class", "sstable-background")
            .attr("width", 100)
            .attr("height", 50)
            .attr("rx", 5)
            .attr("ry", 5);

        // MERGE
        const merged = enter.merge(selection);

        // If you want to animate them repositioning if a new SStable appears, do:
        merged.transition()
            .duration(500)
            .attr("transform", (d, i) => `translate(${margin + i * sstableSpacing}, 140)`);
    }

    updateAllEntries() {
        // 1. Let’s separate items by stage in memory, so we know their index in each stage
        const memtableItems = this.allEntries.filter(d => d.stage === 'memtable');
        const level0Items = this.allEntries.filter(d => d.stage === 'level0');
        const mergeZoneItems = this.allEntries.filter(d => d.stage === 'mergeZone');
        // ... etc. if you have more stages

        // We'll use these arrays to figure out each item's index in its stage. 
        // Typically you might have them sorted by key if needed.

        // 2. The data join (all items in a single selection)
        const selection = this.svg.selectAll(".lsm-entry")
            .data(this.allEntries, d => d.key); // key function ensures stable matching

        // EXIT
        selection.exit()
            .transition()
            .duration(300)
            .style("opacity", 0)
            .remove();

        // ENTER
        const enterSel = selection.enter()
            .append("g")
            .attr("class", "lsm-entry element")
            .attr("transform", d => {
                // Place them initially at their final position or somewhere else if you prefer
                const { x, y } = this.computePosition(d, memtableItems, level0Items, mergeZoneItems);
                return `translate(${x}, ${y})`;
            });

        // A. Append circle with zero radius and the .memory-buffer class so it’s visible
        enterSel.append("circle")
            .attr("class", "memory-buffer")
            .attr("r", 0);

        // B. Append text
        enterSel.append("text")
            .attr("text-anchor", "middle")
            .attr("dy", "0.3em")
            .text(d => `${d.key}:${d.value}`);

        // C. Chain the transitions **on the enter selection** so new items go:
        //    1) from opacity=0 to 1
        //    2) from r=0 to r=elementSize/2
        //    3) (Optionally) remain in the same transform if you want them
        //       to appear in place. If you want them to move from an off-screen
        //       position, you'd set an initial transform, then update it here.
        enterSel
            .transition()
            .duration(300)
            .select("circle")
            .attr("r", this.config.elementSize / 2);

        // MERGE (ENTER + UPDATE)
        const mergedSel = selection.merge(enterSel);

        // D. Now for existing + new items, we do the final position transition.
        //    This won't override the enter fade, because new items have already
        //    set their opacity=1. Meanwhile, old items also move to their new
        //    positions. 
        mergedSel.transition()
            .duration(500)
            .attr("transform", d => {
                const { x, y } = this.computePosition(d, memtableItems, level0Items, mergeZoneItems);
                return `translate(${x}, ${y})`;
            });
    }

    async handleBeforeMemtableInsert(data) {
        // Could add pre-insertion animations here
    }

    async handleAfterMemtableInsert(data) {
        await this.animationQueue.add(async () => {
            const { memtable } = data;
            // "memtable" is the latest array of entries in the memtable (sorted).

            // 1. Remove all old items in allEntries with stage='memtable'
            this.allEntries = this.allEntries.filter(d => d.stage !== 'memtable');

            // 2. Insert the new memtable items:
            //    We can either keep the old .key if it matches, or we just create new objects.  
            //    If you want stable references for the same keys, you can do a more advanced approach,
            //    but here's a simple pattern: just create a new object with stage='memtable' for each item.
            const newMemtableEntries = memtable.map(entry => ({
                key: entry.key,
                value: entry.value,
                stage: 'memtable'
            }));

            // 3. Add them to the unified array
            this.allEntries.push(...newMemtableEntries);

            // 4. Call updateAllEntries so D3 re-binds data and transitions
            this.updateAllEntries();

            // 5. Optionally wait for some time or let the transitions finish
            await new Promise(resolve => setTimeout(resolve, 500));
        });
    }


    async handleMemtableFlushed(data) {
        await this.animationQueue.add(async () => {
            const { memtableState, level0State } = data;
            // level0State is an array of SStable objects

            // A. The newly created SStable is the last in level0State
            const newSstableObject = level0State[level0State.length - 1];
            const sstableId = level0State.length - 1;  // or a unique ID

            // B. Update your bounding-box array (or object) for level0
            //    This data structure will let you do a separate join for the rectangles
            //    Each SStable can store an ID, plus metadata if you want to show size, timestamps, etc.
            this.level0Sstables = level0State.map((sst, i) => {
                return {
                    id: i,  // or sst.id if you have a unique ID in the backend
                    // Possibly store the entire sst object or just metadata
                    numEntries: sst.data.length
                    // etc. 
                };
            });

            // C. Switch entries from 'memtable' to 'level0' in your single allEntries array.
            //    We'll set sstableId so we know which bounding box they're in.
            this.allEntries
                .filter(d => d.stage === 'memtable')
                .forEach(d => {
                    d.stage = 'level0';
                    d.sstableId = sstableId; // So we can offset them within that box
                });

            // D. Call updateSstables to draw bounding boxes
            this.updateSstables();

            // E. Call updateAllEntries so entries animate to their new positions.
            this.updateAllEntries();

            await new Promise(r => setTimeout(r, 500));
        });
    }


    async handleBeforeInsert(data) {
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
            const { sourceLevel, targetLevel } = data;

            // Get exact level positions from config
            const level1Y = 140; // From the DOM inspector

            // Get the entire source level group
            const sourceGroup = this.svg.select(`.level-group-level${sourceLevel}`);

            // Ensure target level group exists at the correct Y position
            let targetGroup = this.svg.select(`.level-group-level${targetLevel}`);
            if (targetGroup.empty()) {
                targetGroup = this.svg.append("g")
                    .attr("class", `level-group-level${targetLevel}`)
                    .attr("transform", `translate(20, ${level1Y})`);  // x=20 from DOM inspector
            }

            // Move the entire group and its contents
            await new Promise(resolve => {
                sourceGroup
                    .transition()
                    .duration(200)
                    .attr("transform", `translate(20, ${level1Y})`)
                    .on("end", resolve);
            });

            await new Promise(resolve => setTimeout(resolve, 200));
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

    async handleMergeGroupFound(data) {
        await this.animationQueue.add(async () => {
            // Remove any existing highlights first
            this.svg.selectAll(".merging").classed("merging", false);

            // Highlight source table
            const sourceSelector = `.level-group-level${data.sourceTable.level}`;

            // Select only the SSTable at the specific index
            this.svg.selectAll(`${sourceSelector} .sstable-group`)
                .filter((d, i) => i === data.sourceTable.index)
                .select("rect")  // Get the rectangle within the matched SSTable group
                .classed("merging", true);

            // Highlight overlapping tables
            data.overlappingTables.forEach(table => {
                const targetSelector = `.level-group-level${table.level}`;

                // Select only the SSTable at the specific index
                this.svg.selectAll(`${targetSelector} .sstable-group`)
                    .filter((d, i) => i === table.index)
                    .select("rect")
                    .classed("merging", true);
            });

            await new Promise(resolve => setTimeout(resolve, 200));
        });
    }

    async handleMergeStep(data) {
        await this.animationQueue.add(async () => {
            const { type, leftEntryIndex, rightEntryIndex, mergedSoFar } = data;
            const elementSize = this.config.elementSize;
            const entrySpacing = 15;
            // Use the merge zone’s left offset (same as the background's x)
            const mergeZoneMargin = this.config.margin.left;

            // Select the merge zone (which should have been set up already)
            const mergeZone = this.svg.select('.merge-zone');
            if (mergeZone.empty()) {
                throw new Error("Merge zone not found. Please call handleSetupMergeZone first.");
            }

            // Calculate target X based on how many elements are already in the merge zone.
            // (Assuming that reparented entries have a class "element" on them.)
            const existingEntries = mergeZone.selectAll(".element").size();
            const targetX = mergeZoneMargin + (existingEntries * (elementSize + entrySpacing));

            // Choose the source group based on the event type:
            // - "takeLeft": entry is in level1 group
            // - "takeRight": entry is in level0 group
            let sourceGroupSelector, entryIndex;
            if (type === 'takeLeft') {
                sourceGroupSelector = '.level-group-level1';
                entryIndex = leftEntryIndex;
            } else {
                sourceGroupSelector = '.level-group-level0';
                entryIndex = rightEntryIndex;
            }

            const entry = this.svg.select(`${sourceGroupSelector} .memtable-entry-${entryIndex}`);
            if (entry.empty()) {
                console.warn(`Entry not found for index: ${entryIndex}`);
                return;
            }

            // Reparenting the entry into the merge zone while preserving its visual position:
            const node = entry.node();
            const oldCTM = node.getCTM();
            const newParentCTM = mergeZone.node().getCTM().inverse();
            const newCTM = newParentCTM.multiply(oldCTM);

            // Remove from the old group and append into the merge zone.
            // (Make sure the entry has a persistent class "element" so we can count it later.)
            entry.remove();
            const reparentedEntry = mergeZone.append(() => node)
                .attr("class", d => d ? d.className.baseVal : "element") // Preserve or add the "element" class
                .attr("transform", `matrix(${newCTM.a}, ${newCTM.b}, ${newCTM.c}, ${newCTM.d}, ${newCTM.e}, ${newCTM.f})`);

            // Animate the entry to its target position within the merge zone.
            await new Promise(resolve => {
                reparentedEntry
                    .transition()
                    .duration(500)
                    .attr("transform", `translate(${targetX}, 0)`)
                    .on("end", resolve);
            });

            // Brief pause after the merge step animation
            await new Promise(resolve => setTimeout(resolve, 100));
        });
    }

    async handleSetupMergeZone(data) {
        await this.animationQueue.add(async () => {
            const { leftTable, rightTable, targetLength } = data;
            const mergeZoneY = 200;  // Position below both levels

            // Create merge zone group
            let mergeZone = this.svg.select('.merge-zone');
            if (mergeZone.empty()) {
                mergeZone = this.svg.append("g")
                    .attr("class", "merge-zone")
                    .attr("transform", `translate(0, ${mergeZoneY})`);

                // Add background rectangle to visualize the zone
                mergeZone.append("rect")
                    .attr("class", "merge-zone-bg")
                    .attr("x", this.config.margin.left - 10)
                    .attr("y", -10)
                    .attr("width", (targetLength * (this.config.elementSize + 10)) + 20)
                    .attr("height", this.config.elementSize + 20)
                    .attr("rx", 5)
                    .attr("ry", 5)
                    .style("fill", "none")
                    .style("stroke", "#666")
                    .style("stroke-width", 2)
                    .style("opacity", 0)
                    .transition()
                    .duration(500)
                    .style("opacity", 1);
            }

            await new Promise(resolve => setTimeout(resolve, 500));
        });
    }
    async handleCleanupMergeZone(data) {
        await this.animationQueue.add(async () => {
            const { mergedEntries, finalPosition } = data; // Available if needed for further animations
            const level1Y = 140;  // Final Y position for level 1
            const marginLeft = this.config.margin.left;
            const elementSize = this.config.elementSize;
            const entrySpacing = 15;

            // Select the merge zone group
            const mergeZone = this.svg.select('.merge-zone');
            if (mergeZone.empty()) {
                console.warn("Merge zone not found; nothing to clean up.");
                return;
            }

            // Interrupt any ongoing transitions on the merge zone and animate it to level1's Y position
            mergeZone.interrupt();
            await new Promise(resolve => {
                mergeZone
                    .transition()
                    .duration(500)
                    .attr("transform", `translate(0, ${level1Y})`)
                    .on("end", resolve);
            });

            // Count how many merged entries already exist in any level1 group
            let existingEntriesCount = 0;
            this.svg.selectAll('.level-group-level1 .element').each(function () {
                existingEntriesCount++;
            });

            // Compute the new group's starting X position so it's appended to the right
            const newGroupX = marginLeft + (existingEntriesCount * (elementSize + entrySpacing));

            // Create a new level1 group for the merged entries
            const newLevel1Group = this.svg.append("g")
                .attr("class", "level-group-level1")
                .attr("transform", `translate(${newGroupX}, ${level1Y})`);

            // Reparent each entry from the merge zone to the new level1 group
            mergeZone.selectAll(".element").each(function () {
                const node = this;
                const entry = d3.select(this);

                // Compute the element’s current absolute transform
                const oldCTM = node.getCTM();

                // Get the new parent’s inverse CTM so we can convert coordinates
                const newParentCTM = newLevel1Group.node().getCTM().inverse();

                // Multiply to get the transform relative to the new level1 group
                const newCTM = newParentCTM.multiply(oldCTM);

                // Remove and reappend the element with its new transform
                entry.remove();
                newLevel1Group.append(() => node)
                    .attr("transform", `matrix(${newCTM.a}, ${newCTM.b}, ${newCTM.c}, ${newCTM.d}, ${newCTM.e}, ${newCTM.f})`);
            });

            // Remove the merge zone group
            mergeZone.remove();

            // Brief pause after cleanup
            await new Promise(resolve => setTimeout(resolve, 100));
        });
    }
}

// 5. Configuration and Setup
const config = {
    maxMemtableSize: 4,
    maxElementsPerLevel: [4, 8, 16],
    elementSize: 15,  // reduced to 15 (from 30)
    levelHeight: 45,  // reduced to 45 (from 90)
    margin: { top: 20, right: 20, bottom: 20, left: 20 },
    print: true,
    width: 800,
    height: 400,
    container: "#lsm-svg"
};

// Initialize
const lsmTree = new LSMTree(config);
const visualizer = new LSMTreeVisualizer(lsmTree, "#lsm-svg", config);

// Wire up UI controls
const demoSequence = [
    // First batch - first range (10-20)
    { key: 40, value: 'a' },
    { key: 15, value: 'b' },
    { key: 20, value: 'c' },

    // Second batch - second range (40-60)
    { key: 10, value: 'd' },
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

// Initial setup - wrap in async IIFE
// (async () => {
//     for (const { key, value } of demoSequence) {
//         await lsmTree.insert(key, value);
//         currentIndex++;
//         if (key === 50 && value === 't') {
//             break;
//         }
//     }
// })();

// Event listener should also be async
document.getElementById("insertBtn").addEventListener("click", async () => {
    if (currentIndex < demoSequence.length) {
        const { key, value } = demoSequence[currentIndex++];
        console.log(`Inserting ${key}:${value} (${currentIndex}/${demoSequence.length})`);
        await lsmTree.insert(key, value);
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

async function testLSMTreeDeepLevelMerge() {
    console.log("=== Testing LSM Tree Deep Level Merge ===");

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
        return false;
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

    return correct;
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
        return false;
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

    return correct;
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

    return correct;
}

function testMergeSSTablesNew() {
    console.log("=== Testing New SSTable Merge ===");

    // Create test case from the example
    const l0 = [
        new SSTable([
            new Entry(9, 'a'),
            new Entry(10, 'b'),
            new Entry(11, 'c')
        ]),
        new SSTable([
            new Entry(12, 'd'),
            new Entry(13, 'e'),
            new Entry(14, 'f'),
            new Entry(15, 'g')
        ])
    ];

    const l1 = [
        new SSTable([
            new Entry(8, 'h'),
            new Entry(9, 'i'),
            new Entry(10, 'j')
        ]),
        new SSTable([
            new Entry(11, 'k'),
            new Entry(12, 'l'),
            new Entry(13, 'm'),
            new Entry(14, 'n'),
            new Entry(15, 'o'),
            new Entry(16, 'p')
        ]),
        new SSTable([
            new Entry(20, 'q'),
            new Entry(21, 'r')
        ])
    ];

    console.log("Input L0:");
    l0.forEach(sst => console.log(sst.toString()));
    console.log("\nInput L1:");
    l1.forEach(sst => console.log(sst.toString()));

    const merged = mergeSSTables(l0, l1);

    console.log("\nMerged Result:");
    merged.forEach(sst => console.log(sst.toString()));

    // Verify the results
    const expected = [
        // First merged table (8-16)
        new SSTable([
            new Entry(8, 'h'),
            new Entry(9, 'a'),
            new Entry(10, 'b'),
            new Entry(11, 'c'),
            new Entry(12, 'd'),
            new Entry(13, 'e'),
            new Entry(14, 'f'),
            new Entry(15, 'g'),
            new Entry(16, 'p')
        ]),
        // Unchanged last table (20-21)
        new SSTable([
            new Entry(20, 'q'),
            new Entry(21, 'r')
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

    return correct;
}

// Run all tests
(async () => {
    const testResults = {
        "LSM Tree Deep Level Merge": await testLSMTreeDeepLevelMerge(),
        "LSM Tree Merge Compaction": await testLSMTreeMergeCompaction(),
        "LSM Tree Level Merge": await testLSMTreeLevelMerge(),
        "New SSTable Merge": await testMergeSSTablesNew()
    };

    console.log("\n=== Test Summary ===");
    let allPassed = true;
    Object.entries(testResults).forEach(([testName, passed]) => {
        console.log(`${testName}: ${passed ? "✅ PASSED" : "❌ FAILED"}`);
        if (!passed) allPassed = false;
    });
    console.log("==================");

    if (!allPassed) {
        console.error("Some tests failed!");
    } else {
        console.log("All tests passed!");
    }
})();

