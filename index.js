/* TODO: move to independent module "pico-repo" as this file was cannibalized from PoH
 * So this is going to be a persistent block-store
 * as opposed to picofeed which is an in-memory datatype,
 *
 */
const Feed = require('picofeed')
const REPO_SYMBOL = Symbol.for('PicoRepo')

// Namespaces
const HEAD = 0 // Feed end tag by Author PK
const BLOCK = 1 // Block contents
const TAIL = 2 // Feed start tag by Author PK
const LATEST = 3 // Last write of Author PK
const REG = 4 // misc application namespace/userland
// Attempt to keep track of individual feeds in store
// using the signature of their genesis block as "chainID"
const CHAIN_TAIL = 5 // Feed start tag by GenesisBlock Signature
const CHAIN_HEAD = 6 // Feed end tag inverse index of CHAIN_TAIL

function mkKey (type, key) {
  if (!Buffer.isBuffer(key)) throw new Error('Expected key to be a Buffer')
  const buffer = Buffer.alloc(1 + key.length)
  key.copy(buffer, 1)
  buffer[0] = type
  return buffer
}

class PicoRepo { //  PicoJar (a jar for crypto-pickles)
  static isRepo (o) { return o && o[REPO_SYMBOL] }

  constructor (db, strategy = []) {
    this[REPO_SYMBOL] = true
    this._db = db

    // Experimental flag
    this.allowDetached = false

    // A jar usually boasts a label describing it's contents
    // so that people know what to expect to get and at least
    // what not to accidentally put.
    this._strategies = Array.isArray(strategy) ? strategy : [strategy]
    for (const s of this._strategies) {
      if (typeof s !== 'function') throw new Error('ExpectedCallback')
    }
  }

  get mkKey () { return mkKey } // minimalistic sub-leveldown alternative

  async _setHeadPtr (name, ptr) {
    const key = mkKey(HEAD, name)
    await this._db.put(key, ptr)
    return ptr
  }

  async _setLatestPtr (name, ptr) {
    const key = mkKey(LATEST, name)
    await this._db.put(key, ptr)
    return ptr
  }

  async _getTag (type, name) {
    const key = mkKey(type, name)
    const buffer = await this._db.get(key)
      .catch(err => {
        if (!err.notFound) throw err
      })
    return buffer
  }

  async _getHeadPtr (name) { return this._getTag(HEAD, name) }

  async _getTailPtr (name) { return this._getTag(TAIL, name) }

  async _getLatestPtr (name) { return this._getTag(LATEST, name) }

  async _getChainPtr (name) { return this._getTag(CHAIN_TAIL, name) }

  async writeBlock (block) {
    const key = mkKey(BLOCK, block.sig)
    // TODO: this method used to return false when block exists
    const buffer = Buffer.alloc(32 + block.buffer.length)
    block.key.copy(buffer, 0)
    block.buffer.copy(buffer, 32)

    const batch = []
    batch.push({ type: 'put', key, value: buffer })

    if (block.isGenesis) {
      batch.push({ type: 'put', key: mkKey(TAIL, block.key), value: block.sig })
      batch.push({ type: 'put', key: mkKey(CHAIN_TAIL, block.sig), value: block.sig })
      batch.push({ type: 'put', key: mkKey(CHAIN_HEAD, block.sig), value: block.sig })
    } else {
      // Move ChainID tag
      const prevKey = mkKey(CHAIN_TAIL, block.parentSig)
      const chainId = await this._db.get(prevKey)
      batch.push({ type: 'del', key: prevKey })
      batch.push({ type: 'put', key: mkKey(CHAIN_TAIL, block.sig), value: chainId })
      batch.push({ type: 'put', key: mkKey(CHAIN_HEAD, chainId), value: block.sig })
    }
    await this._db.batch(batch)
    return true
  }

  async readBlock (id) {
    const key = mkKey(BLOCK, id)
    const buffer = await this._db.get(key)
      .catch(err => {
        if (!err.notFound) throw err
      })

    if (buffer) return Feed.mapBlock(buffer, 32, buffer.slice(0, 32))
  }

  // Internal because it performs a full block read, use with care.
  // (blocks are small in this application, don't care)
  async _hasBlock (id) {
    return !!(await this.readBlock(id))
  }

  async _traceOwnerOf (sig) {
    // Tricky... trading off some extra index memory could possibly
    // reduce the need to traverse the chain backwards
    // I don't know.
    let n = 0
    for await (const block of this._chainLoad(sig)) {
      if (block.isGenesis) return block.key
      n++
    }
    if (n) throw new Error('Orphaned Chain')
    // else chain not found
  }

  /**
   * Returns entire feed given a blockId of the set.
   * ( Different from loadFeed(ptr) as loadFeed just dumbly loads backwards
   * until stop hit. resolveFeed(ptr) uses the CHAIN indices
   * to fetch full chain
   */
  async resolveFeed (sig, stopCallback = undefined) {
    // O-1 constant lookup via TAIL tag
    let tip = await this._getTag(CHAIN_TAIL, sig)
    // Assuming that sig === CHAIN_HEAD(CHAIN_TAIL(sig))
    if (tip) return this.loadFeed(sig, stopCallback)

    // Traverse backwards to find HEAD tag
    for await (const block of this._chainLoad(sig)) {
      if (!block.isGenesis) continue
      tip = await this._getTag(CHAIN_HEAD, block.sig)
    }
    if (!tip) throw new Error('FeedNotFound')
    return this.loadFeed(tip, stopCallback)
  }

  async merge (feed, strategy) { // or merge() ?
    if (!Feed.isFeed(feed)) feed = Feed.from(feed)
    if (!feed.length) return 0
    let blocksWritten = 0
    let owner = null
    const first = feed.first
    if (first.isGenesis) owner = first.key
    else owner = await this._traceOwnerOf(first.parentSig)
    if (!owner) throw new Error('CannotMerge: Unknown Chain')

    for (const block of feed.blocks()) {
      const author = block.key
      const id = block.sig

      // Ignore existing blocks
      if (await this._hasBlock(id)) continue

      const bumpHead = async () => {
        await this.writeBlock(block)
        await this._setHeadPtr(owner, block.sig)
        await this._setLatestPtr(author, block.sig)
        blocksWritten++
      }

      // fetch current head
      const head = await this._getHeadPtr(owner)

      // Allow creation of new heads
      if (!head) {
        await bumpHead()
        continue
      }

      // skip ahead when previous head points to parent of new block
      if (head.equals(block.parentSig)) {
        await bumpHead()
        continue
      }

      // Check if the gateway to hell is open,
      // intentions were good.
      if (
        this.allowDetached && (
          block.isGenesis ||
          await this._hasBlock(block.parentSig)
        )
      ) {
        await bumpHead()
        continue
      }

      // Abort if no verifiably common parent in repo
      // console.debug('haveParent?: ', block.parentSig.hexSlice())
      if (!(await this._hasBlock(block.parentSig))) break

      // Fast-forward when previous head is an ancestor
      let isAncestor = false
      for await (const parentBlock of this._chainLoad(block.parentSig)) {
        if (head.equals(parentBlock.sig)) {
          isAncestor = true
          break // break inner _chainLoad loop
        }
      }
      if (isAncestor) {
        await bumpHead()
        continue
      }

      // Let user-logic decide if this block should
      // be accepted as a new head
      if (await this._queryUserStrategy(block, strategy)) {
        await bumpHead()
        continue
      }
      break // No strategy allowed this transition, abort merge
    }
    return blocksWritten
  }

  async _queryUserStrategy (block, inlineStrategy) {
    for (const strategy of [inlineStrategy, ...this._strategies]) {
      if (!strategy) continue
      const p = strategy(block, this)
      if (typeof p.then !== 'function') throw new Error('PromiseExpected')
      if (await p) {
        return true
      }
    }
    return false
  }

  /**
   * Loads feed from pubkey's personal feed
   */
  async loadHead (key, stopCallback = undefined) {
    const head = await this._getHeadPtr(key)
    if (head) return this.loadFeed(head, stopCallback)
  }

  /**
   * Loads feed from pubkey's latest block and backwards
   */
  async loadLatest (key, stopCallback = undefined) {
    const head = await this._getLatestPtr(key)
    if (head) return this.loadFeed(head, stopCallback)
  }

  async loadFeed (ptr, stopCallback = undefined) {
    let limit = 0
    if (Number.isInteger(stopCallback) && stopCallback > 0) limit = stopCallback
    if (limit) stopCallback = (_, after) => !--limit && after(true)
    const pending = []
    for await (const block of this._chainLoad(ptr)) {
      /* If needed async abort use this instead.
      if (typeof stopCallback === 'function') {
        const abort = await defer(d => stopCallback(d.bind(null, null)))
        if (abort) break
      }
      */
      let abort = false
      let abortAfter = false
      if (typeof stopCallback === 'function') {
        stopCallback(block, after => {
          if (after) abortAfter = true
          else abort = true
        })
      }

      // Append block to in-memory feed
      if (!abort) {
        pending.unshift(block)
      }

      // Break loop to stop the chainloader
      if (abortAfter || abort) break
    }

    // Reconstruct feed from blocks
    const feed = Feed.fromBlockArray(pending)
    if (feed.length) return feed
  }

  async * _chainLoad (next) {
    let first = true
    while (true) {
      const block = await this.readBlock(next)
      if (!block && first) break // chain not found, silent
      first = false
      if (!block) throw new Error('ParentNotFound') // broken chain, loud
      yield block
      if (block.isGenesis) break // We've hit a genesis block
      next = block.parentSig
    }
  }

  /**
   * If allowDetached is true this method expects CHAIN pointers
   * instead of AUTHOR/HEAD pointers.
   * @return A feed containing all blocks that were evicted or null if nothing was removed
   */
  async rollback (head, ptr) {
    let stopHit = false
    const loader = !this.allowDetached ? this.loadHead : this.resolveFeed

    const evicted = await loader.bind(this)(head, (block, abort) => {
      if (ptr && block.sig.equals(ptr)) {
        stopHit = true
        abort()
      }
    })
    if (ptr && !stopHit) throw new Error('ReferenceNotFound')
    if (!evicted) return null

    const batch = []
    const relocate = []
    const latest = {}
    for (let i = evicted.length - 1; i >= 0; i--) {
      const block = evicted.get(i)
      const key = block.key.toString('hex')
      if (!latest[key]) latest[key] = await this._getLatestPtr(block.key)
      // detect if latest-ptr needs to be adjusted
      if (latest[key]?.equals(block.sig)) {
        relocate.push(block.key)
      }
      // Delete block op
      batch.push({ type: 'del', key: mkKey(BLOCK, block.sig) })
    }

    /*
     * Latest tags are not used atm. maybe let's just relocate them to
     * their respective heads after rollback, it's not correct but
     * better than nothing.
     * Maybe deprecate @latest in 1.7stack release
     */
    /*
    const newTags = []
    for await (const block of this._chainLoad(ptr)) {
      if (!relocate.length) break
      const idx = relocate.findIndex(k => k.equals(block.key))
      if (~idx) {
        newTags.push({
          key: relocate[idx],
          ptr: block.sig
        })
        relocate.splice(idx, 1)
      }
    } */

    const isFeedPurged = evicted.first.isGenesis
    // Clean up author-tail tag on full eviction
    if (isFeedPurged && !this.allowDetached) {
      batch.push({ type: 'del', key: mkKey(TAIL, head) })
    } else if (isFeedPurged) { // Remove dangling AuthorTails (useless)
      const k = evicted.first.key
      const currentTail = await this._getTailPtr(k)
      if (currentTail?.equals(evicted.first.sig)) {
        batch.push({ type: 'del', key: mkKey(TAIL, k) })
      }
      // TODO: relocate to other chain of same author
    }

    // -- Adjust new head (heads roll unconditionally)
    if (!this.allowDetached) {
      batch.push({ type: 'del', key: mkKey(HEAD, head) })
      if (!isFeedPurged) { // Partial Rollback, part of feed still exists
        batch.push({ type: 'put', key: mkKey(HEAD, head), value: ptr })
      }
    } else { // remove dangling AuthorHeads
      const k = evicted.first.key
      const currentHead = await this._getHeadPtr(k)
      if (currentHead?.equals(evicted.last.sig)) {
        batch.push({ type: 'del', key: mkKey(HEAD, k) })
      }
      // TODO: relocate to other chain of same author
    }

    // Relocated chain-identifiers
    batch.push({ type: 'del', key: mkKey(CHAIN_TAIL, evicted.last.sig) })
    const chainId = await this._getTag(CHAIN_TAIL, evicted.last.sig)

    if (chainId) batch.push({ type: 'del', key: mkKey(CHAIN_HEAD, chainId) })

    if (chainId && !isFeedPurged) {
      batch.push({
        type: 'put',
        key: mkKey(CHAIN_TAIL, evicted.first.parentSig),
        value: chainId
      })
      batch.push({
        type: 'put',
        key: mkKey(CHAIN_HEAD, chainId),
        value: evicted.first.parentSig
      })
    }

    // For now, purge dangling 'latest'-tags
    for (const key of relocate) {
      batch.push({ type: 'del', key: mkKey(LATEST, key) })
    }

    await this._db.batch(batch)
    return evicted
  }

  // Expose some wrappers to avoid invoking internal API directly
  get headOf () { return this._getHeadPtr }
  get tailOf () { return this._getTailPtr }
  get latestOf () { return this._getLatestPtr }
  get chainload () { return this._chainLoad }
  get ownerOf () { return this._traceOwnerOf }

  /* Signatures are keys are 32bytes in size and are duplicated
   * into every block blob, an idea was to use a varint keyId to make each
   * block 28bytes smaller on disk. Buuuuut:
   * Let's waste some storage for alpha version.
  async _readCounter (keepOpen = false) {
    const file = this._store('key_counter')
    const val = await defer(d => file.read(0, 64, d))
      .then(b => b.readUInt32BE())
      .catch(err => {
        console.warn('Failed reading counter, resetting to 0', err)
        return 0
      })
    if (this._autoClose && !keepOpen) await defer(d => file.close(d))
    return val
  }

  async _incrementCounter () {
    const file = this._store('key_counter')
    let val = await this._readCounter(true)
    const buf = Buffer.alloc(4)
    buf.writeUInt32BE(++val)
    await defer(d => file.write(0, buf, d))
    if (this._autoClose) await defer(d => file.close(d))
    return val
  }
  */

  async writeReg (key, value) {
    const bkey = mkKey(REG, Buffer.from(key))
    await this._db.put(bkey, Buffer.from(value))
    return true
  }

  async readReg (key) {
    const bkey = mkKey(REG, Buffer.from(key))
    const value = await this._db.get(bkey)
      .catch(err => {
        if (!err.notFound) throw err
      })
    return value
  }

  async listHeads () {
    return this._listRange({
      gt: mkKey(HEAD, Buffer.alloc(32).fill(0)),
      lt: mkKey(HEAD, Buffer.alloc(32).fill(0xff))
    })
  }

  async listTails () {
    return this._listRange({
      gt: mkKey(TAIL, Buffer.alloc(32).fill(0)),
      lt: mkKey(TAIL, Buffer.alloc(32).fill(0xff))
    })
  }

  async listLatest () {
    return this._listRange({
      gt: mkKey(LATEST, Buffer.alloc(32).fill(0)),
      lt: mkKey(LATEST, Buffer.alloc(32).fill(0xff))
    })
  }

  /**
   * Returns a list of unique feeds in repo.
   * [blockPtr, chainId+(length?)]
   */
  async listFeeds () {
    return this._listRange({
      gt: mkKey(CHAIN_TAIL, Buffer.alloc(32).fill(0)),
      lt: mkKey(CHAIN_TAIL, Buffer.alloc(32).fill(0xff))
    })
  }

  async listFeedHeads () {
    return this._listRange({
      gt: mkKey(CHAIN_HEAD, Buffer.alloc(32).fill(0)),
      lt: mkKey(CHAIN_HEAD, Buffer.alloc(32).fill(0xff))
    })
  }

  async _listRange (query) {
    const iter = this._db.iterator(query)
    const result = []
    while (true) {
      try {
        const [key, value] = await new Promise((resolve, reject) => {
          iter.next((err, key, value) => {
            if (err) reject(err)
            else resolve([key, value])
          })
        })
        if (!key) break
        result.push({ key: key.slice(1), value })
      } catch (err) {
        console.warn('Iterator died with an error', err)
        break
      }
    }
    await new Promise((resolve, reject) => {
      iter.close(err => err ? reject(err) : resolve())
    })
    return result
  }
}

module.exports = PicoRepo
