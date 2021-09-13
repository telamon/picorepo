// SPDX-License-Identifier: AGPL-3.0-or-later
const test = require('tape')
const Feed = require('picofeed')
const levelup = require('levelup')
const memdown = require('memdown')
const Repo = require('.')
const DB = () => levelup(memdown())

test('PicoRepo: low-level block store', async t => {
  try {
    const repo = new Repo(DB())
    const f = new Feed()
    const { pk, sk } = Feed.signPair()
    const author = pk
    f.append('hello', sk)
    const ptr = await repo._setHeadPtr(author, f.last.sig)
    t.ok(ptr.equals(await repo._getHeadPtr(author)))

    const blockId = f.last.sig

    t.ok(await repo.writeBlock(f.last))
    // t.notOk(await repo.writeBlock(f.last)) // Not sure if want

    const storedBlock = await repo.readBlock(blockId)
    t.ok(f.last.buffer.equals(storedBlock.buffer))
    t.ok(f.last.key.equals(storedBlock.key))

    const deleted = await repo.deleteBlock(blockId)
    t.ok(deleted)
    const notFound = await repo.readBlock(blockId)
    t.notOk(notFound)
  } catch (err) { t.error(err) }
  t.end()
})

// Test linear single author fast-forward
// bumpHead() if (new block.parentSig === currentHead)
test('PicoRepo: store and get feed', async t => {
  try {
    const repo = new Repo(DB())
    const f = new Feed()
    const { pk, sk } = Feed.signPair()
    f.append('Hello', sk)
    f.append('World', sk)
    let stored = await repo.merge(f)
    t.equal(stored, 2)

    f.append('Slice is good', sk)
    stored = await repo.merge(f)
    t.equal(stored, 1)

    const restored = await repo.loadHead(pk)
    t.ok(restored)
    t.ok(restored.last.sig.equals(f.last.sig))
    t.equal(restored.length, f.length)
  } catch (err) { t.error(err) }
  t.end()
})

// bumpHead() when currentHead is ancestor of block
test('PicoRepo: linear multi-author fast-forward', async t => {
  try {
    const repo = new Repo(DB())
    const f = new Feed()
    const a = Feed.signPair()
    const b = Feed.signPair()
    f.append('A0', a.sk)
    f.append('A1->A0', a.sk)
    let stored = await repo.merge(f)
    t.equal(stored, 2)

    f.append('B0->A1', b.sk)
    stored = await repo.merge(f)
    t.equal(stored, 1)

    f.append('A2->B0', a.sk)
    stored = await repo.merge(f)
    t.equal(stored, 1)
  } catch (err) { t.error(err) }
  t.end()
})
