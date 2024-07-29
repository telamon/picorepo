/* Pico Repo(sitory)
 * This is a persistent block-store
 * as opposed to picofeed which is an in-memory datatype,
 * @author Tony Ivanov <tony@decentlabs.se>
 * @license AGPLv3
 *
 * This code was written back in 2020,
 * During Feed 4.x upgrade I realize this is due for a rewrite
 */
import {
  Feed,
  Block,
  feedFrom,
  toU8,
  cpy,
  cmp,
  toHex,
  usize
} from 'picofeed'
const REPO_SYMBOL = Symbol.for('PicoRepo')
/** @typedef {(block: Block, self: Repo) => boolean|Promise<boolean>} MergeStrategy */
/** @typedef {import('picofeed').SignatureBin} SignatureBin */
/** @typedef {import('picofeed').PublicKey} PublicKey */
/** @typedef {import('abstract-level').AbstractLevel<any,Uint8Array,Uint8Array>} BinaryLevel */
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

// export function isBuffer (b) { return b instanceof ArrayBuffer || ArrayBuffer.isView(b) }

function mkKey (type, key) {
  key = toU8(key)
  // if (!isBuffer(key)) throw new Error('Expected key to be a Buffer')
  const buffer = new Uint8Array(1 + key.length)
  cpy(buffer, key, 1)
  buffer[0] = type
  return buffer
}

export class Repo {
  /** @type {(o: *) => o is Repo} */
  static isRepo (o) { return o && o[REPO_SYMBOL] }

  /**
   * @param {BinaryLevel} db Abstract Leveldown adapter
   * @param {MergeStrategy|Array<MergeStrategy>} strategy
   */
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

  /**
   * Stores a block in repository and updates internal references.
   * @param {Block} block
   * @returns {Promise<boolean>}
   */
  async writeBlock (block) {
    const key = mkKey(BLOCK, block.sig)
    // TODO: this method used to return false when block exists
    const buffer = new Uint8Array(block.blockSize)

    if (!block.key) throw new Error('AnonymousBlocks not supported')
    cpy(buffer, block.buffer, 0)

    const batch = []
    batch.push({ type: 'put', key, value: buffer })

    if (block.genesis) {
      batch.push({ type: 'put', key: mkKey(TAIL, block.key), value: block.sig })
      batch.push({ type: 'put', key: mkKey(CHAIN_TAIL, block.sig), value: block.sig })
      batch.push({ type: 'put', key: mkKey(CHAIN_HEAD, block.sig), value: block.sig })
    } else {
      // Move ChainID tag
      const prevKey = mkKey(CHAIN_TAIL, block.psig)
      const chainId = await this._db.get(prevKey)
      batch.push({ type: 'del', key: prevKey })
      batch.push({ type: 'put', key: mkKey(CHAIN_TAIL, block.sig), value: chainId })
      batch.push({ type: 'put', key: mkKey(CHAIN_HEAD, chainId), value: block.sig })
    }
    // @ts-ignore
    await this._db.batch(batch)
    return true
  }

  /**
   * Read block by signature
   * @param {SignatureBin} id Block Signature
   * @returns {Promise<Block|undefined>} requested block if exists
   */
  async readBlock (id) {
    const key = mkKey(BLOCK, id)
    const buffer = await this._db.get(key)
      .catch(err => {
        if (!err.notFound) throw err
      })
    if (buffer) {
      const b = new Block(buffer)
      return b
    }
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
      if (block.genesis) return block.key
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
   * @param {SignatureBin} sig
   */
  async resolveFeed (sig, stopCallback = undefined) {
    // O-1 constant lookup via TAIL tag
    let tip = await this._getTag(CHAIN_TAIL, sig)
    // Assuming that sig === CHAIN_HEAD(CHAIN_TAIL(sig))
    if (tip) return this.loadFeed(sig, stopCallback)

    // Traverse backwards to find HEAD tag
    for await (const block of this._chainLoad(sig)) {
      if (!block.genesis) continue
      tip = await this._getTag(CHAIN_HEAD, block.sig)
    }
    if (!tip) throw new Error('FeedNotFound')
    return this.loadFeed(tip, stopCallback)
  }

  /**
   * Attempts to merge the feed into the repository.
   * If a new 'chain' will be stored or an existing 'chain' extended
   * depends on this._allowDetached set to false.
   * Then each chain is tracked by Author.
   * (One chain per user.)
   *
   * When this._allowDetached is set to true
   * then each chain is tracked by block-id/sig of genesis block.
   *
   * ## Merge Strategy
   * The default strategy is to allow only same-key to extend a chain.
   * Provide a custom strategy callback to enable chains with multiple-authors.
   *
   * @param {Feed|Block|ArrayBuffer|Uint8Array|Array<Block>} feed
   * @param {MergeStrategy} strategy Callback, can be async, return true to merge or false to reject.
   * @returns {Promise<number>} Blocks written
   */
  async merge (feed, strategy) { // or merge() ?
    if (!Feed.isFeed(feed)) feed = feedFrom(feed)
    if (!feed.length) return 0
    let blocksWritten = 0
    let owner = null
    const first = feed.first
    if (first.genesis) owner = first.key
    else owner = await this._traceOwnerOf(first.psig)
    if (!owner) throw new Error('CannotMerge: Unknown Chain')

    for (const block of feed.blocks) {
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
      if (cmp(head, block.psig)) {
        await bumpHead()
        continue
      }

      // Check if the gateway to hell is open,
      // intentions were good.
      if (
        this.allowDetached && (
          block.genesis ||
          await this._hasBlock(block.psig)
        )
      ) {
        await bumpHead()
        continue
      }

      // Abort if no verifiably common parent in repo
      // console.debug('haveParent?: ', block.psig.hexSlice())
      if (!(await this._hasBlock(block.psig))) break

      // Fast-forward when previous head is an ancestor
      let isAncestor = false
      for await (const parentBlock of this._chainLoad(block.psig)) {
        if (cmp(head, parentBlock.sig)) {
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
      if (await strategy(block, this)) {
        return true
      }
    }
    return false
  }

  /**
   * Loads feed where Author produced first block.
   * @param {PublicKey} key Author Key
   */
  async loadHead (key, stopCallback = undefined) {
    const head = await this._getHeadPtr(key)
    if (head) return this.loadFeed(head, stopCallback)
  }

  /**
   * Loads feed where Author produced last block.
   * @param {PublicKey} key Author Key
   */
  async loadLatest (key, stopCallback = undefined) {
    const head = await this._getLatestPtr(key)
    if (head) return this.loadFeed(head, stopCallback)
  }

  /** @typedef {(block: Block, stop: (after: boolean) => void) => void} StopCallback */
  /**
   * Loads a feed from BlockID and backwards
   * @param {SignatureBin} ptr Block id
   * @param {StopCallback|number|undefined} stopCallback load N blocks if number provided
   * @returns {Promise<Feed|undefined>} Loaded feed
   */
  async loadFeed (ptr, stopCallback = undefined) {
    let limit = 0
    if (usize(stopCallback)) limit = stopCallback
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
    const feed = feedFrom(pending)
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
      if (block.genesis) break // We've hit a genesis block
      next = block.psig
    }
  }

  /**
   * If allowDetached is true this method expects CHAIN pointers
   * instead of AUTHOR/HEAD pointers.
   * @param {PublicKey|SignatureBin} head Block Signature or Author's PublicKey
   * @param {SignatureBin?} stopAt Stop at block signature
   * @return {Promise<Feed?>} A feed containing all blocks that were evicted or null if nothing was removed
   */
  async rollback (head, stopAt = undefined) {
    let stopHit = false
    const loader = !this.allowDetached ? this.loadHead : this.resolveFeed

    const evicted = await loader.bind(this)(head, (block, abort) => {
      if (stopAt && cmp(block.sig, stopAt)) {
        stopHit = true
        abort()
      }
    })
    if (stopAt && !stopHit) throw new Error('ReferenceNotFound')
    if (!evicted) return null

    const batch = []
    const relocate = []
    const latest = {}
    for (let i = evicted.length - 1; i >= 0; i--) {
      const block = evicted.block(i)
      const key = toHex(block.key)
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

    const isFeedPurged = evicted.first.genesis
    // Clean up author-tail tag on full eviction
    if (isFeedPurged && !this.allowDetached) {
      batch.push({ type: 'del', key: mkKey(TAIL, head) })
    } else if (isFeedPurged) { // Remove dangling AuthorTails (useless)
      const k = evicted.first.key
      const currentTail = await this._getTailPtr(k)
      if (currentTail && cmp(currentTail, evicted.first.sig)) {
        batch.push({ type: 'del', key: mkKey(TAIL, k) })
      }
      // TODO: relocate to other chain of same author
    }

    // -- Adjust new head (heads roll unconditionally)
    if (!this.allowDetached) {
      batch.push({ type: 'del', key: mkKey(HEAD, head) })
      if (!isFeedPurged) { // Partial Rollback, part of feed still exists
        batch.push({ type: 'put', key: mkKey(HEAD, head), value: stopAt })
      }
    } else { // remove dangling AuthorHeads
      const k = evicted.first.key
      const currentHead = await this._getHeadPtr(k)
      if (currentHead && cmp(currentHead, evicted.last.sig)) {
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
        key: mkKey(CHAIN_TAIL, evicted.first.psig),
        value: chainId
      })
      batch.push({
        type: 'put',
        key: mkKey(CHAIN_HEAD, chainId),
        value: evicted.first.psig
      })
    }

    // For now, purge dangling 'latest'-tags
    for (const key of relocate) {
      batch.push({ type: 'del', key: mkKey(LATEST, key) })
    }
    // @ts-ignore
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
    const bkey = mkKey(REG, key)
    await this._db.put(bkey, value)
    return true
  }

  async readReg (key) {
    const bkey = mkKey(REG, key)
    const value = await this._db.get(bkey)
      .catch(err => {
        if (!err.notFound) throw err
      })
    return value
  }

  async listHeads () {
    return this._listRange({
      gt: mkKey(HEAD, u8fill(32, 0)),
      lt: mkKey(HEAD, u8fill(32, 0xff))
    })
  }

  async listTails () {
    return this._listRange({
      gt: mkKey(TAIL, u8fill(32, 0)),
      lt: mkKey(TAIL, u8fill(32, 0xff))
    })
  }

  async listLatest () {
    return this._listRange({
      gt: mkKey(LATEST, u8fill(32, 0)),
      lt: mkKey(LATEST, u8fill(32, 0xff))
    })
  }

  /**
   * Returns a list of unique feeds in repo.
   * [blockPtr, chainId+(length?)]
   */
  async listFeeds () {
    return this._listRange({
      gt: mkKey(CHAIN_TAIL, u8fill(32, 0)),
      lt: mkKey(CHAIN_TAIL, u8fill(32, 0xff))
    })
  }

  async listFeedHeads () {
    return this._listRange({
      gt: mkKey(CHAIN_HEAD, u8fill(32, 0)),
      lt: mkKey(CHAIN_HEAD, u8fill(32, 0xff))
    })
  }

  async _listRange (query) {
    const iter = this._db.iterator(query)
    const result = []
    while (true) {
      const res = await iter.next()
      if (!res?.length) break
      const [key, value] = res
      result.push({ key: key.slice(1), value })
    }
    await iter.close()
    return result
  }
}

/** @type {(n: number, char: number|undefined) => Uint8Array} */
function u8fill (n, char) {
  const b = new Uint8Array(n)
  if (typeof char !== 'undefined') for (let i = 0; i < b.length; i++) b[i] = char
  return b
}
