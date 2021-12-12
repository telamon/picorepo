/* TODO: move to independent module "pico-repo" as this file was cannibalized from PoH
 * So this is going to be a persistent block-store
 * as opposed to picofeed which is an in-memory datatype,
 *
 */
const Feed = require('picofeed')

const HEAD = 0
const BLOCK = 1
const TAIL = 2
const LATEST = 3
const REG = 4 // misc

function mkKey (type, key) {
  if (!Buffer.isBuffer(key)) throw new Error('Expected key to be a Buffer')
  const buffer = Buffer.alloc(1 + key.length)
  key.copy(buffer, 1)
  buffer[0] = type
  return buffer
}

class PicoRepo { //  PicoJar (a jar for crypto-pickles)
  constructor (db, strategy = []) {
    this._db = db
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

  async _getHeadPtr (name) {
    const key = mkKey(HEAD, name)
    const buffer = await this._db.get(key)
      .catch(err => {
        if (!err.notFound) throw err
      })
    return buffer
  }

  async _getTailPtr (name) {
    const key = mkKey(TAIL, name)
    const buffer = await this._db.get(key)
      .catch(err => {
        if (!err.notFound) throw err
      })
    return buffer
  }

  async _setLatestPtr (name, ptr) {
    const key = mkKey(LATEST, name)
    await this._db.put(key, ptr)
    return ptr
  }

  async _getLatestPtr (name) {
    const key = mkKey(LATEST, name)
    const buffer = await this._db.get(key)
      .catch(err => {
        if (!err.notFound) throw err
      })
    return buffer
  }

  async writeBlock (block) {
    const key = mkKey(BLOCK, block.sig)
    // TODO: this method used to return false when block exists
    const buffer = Buffer.alloc(32 + block.buffer.length)
    block.key.copy(buffer, 0)
    block.buffer.copy(buffer, 32)
    await this._db.put(key, buffer)
    // console.debug('writeBlock: ', key.hexSlice())
    if (block.isGenesis) {
      await this._db.put(mkKey(TAIL, block.key), block.sig)
    }
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

  async deleteBlock (id) {
    const key = mkKey(BLOCK, id)
    // TODO: clean up indexes HEAD, LATEST, TAIL
    // might need to rework this method into rollback(id, newHead)
    await this._db.del(key)
      .catch(err => {
        if (!err.notFound) throw err
      })
    return true
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

  async merge (feed, strategy) { // or merge() ?
    if (!Feed.isFeed(feed)) feed = Feed.from(feed)
    let blocksWritten = 0
    let owner = null
    const first = feed.first
    if (!first) return 0
    else if (first.isGenesis) owner = first.key
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

    // Merge all pending blocks in a forward fashion to avoid
    // duplicate revalidations
    const feed = new Feed()
    for (const block of pending) {
      const merged = feed.merge(block)
      if (!merged) throw new Error('InternalError:CannotRestoreStoredBlock')
    }
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

  // Expose some wrappers to avoid invoking internal API directly
  get headOf () { return this._getHeadPtr }
  get tailOf () { return this._getTailPtr }
  get chainload () { return this._chainLoad }

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
    const query = {
      gt: mkKey(HEAD, Buffer.alloc(32).fill(0)),
      lt: mkKey(HEAD, Buffer.alloc(32).fill(0xff))
    }
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
      iter.end(err => err ? reject(err) : resolve())
    })
    return result
  }
}

module.exports = PicoRepo
