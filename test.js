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

    /* use repo.rollback()
    const deleted = await repo.deleteBlock(blockId)
    t.ok(deleted)
    const notFound = await repo.readBlock(blockId)
    t.notOk(notFound)
    */
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

test('HeadRework; each head keeps track of own chain', async t => {
  /* The Problem: (top down)
   * B0  A0~ C0  D0
   * B1~ A4* C1  D1
   * A1      A2~ A5*
   * B2      C2  D2
   * A3
   * B3
   * A6 <--- repo.loadHead(A) should load A4..A0
   *         if (guest) trace backwards will follow '~'link and give A0 (wrong!)
   *         Modify pico-repo bump-head-algo?
   *         Prob#2. repo.loadHead(B) should load A6..B0
   *
   *
   */
  // In this example, A is a guest in B, C and D's feeds.
  const [A, B, C, D] = Array.from(new Array(4)).map(() => Feed.signPair().sk)
  const repo = new Repo(DB(), async () => true)
  const fA = new Feed()
  const fB = new Feed()
  const fC = new Feed()
  const fD = new Feed()
  fB.append('B0', B)
  fB.append('B1~', B)
  await repo.merge(fB)
  fA.append('A0~', A)
  await repo.merge(fA)
  fB.append('A1', A)
  fB.append('B2', B)
  await repo.merge(fB)
  fC.append('C0', C)
  fC.append('C1', C)
  fC.append('A2~', A)
  fC.append('C2', C)
  await repo.merge(fC)
  fB.append('A3', A)
  fB.append('B3', B)
  await repo.merge(fB)
  fA.append('A4*', A)
  await repo.merge(fA)
  fD.append('D0', D)
  fD.append('D1', D)
  fD.append('A5*', A)
  fD.append('D2', D)
  await repo.merge(fD)
  fB.append('A6', C)
  await repo.merge(fB.slice(-1))
  const hA = await repo.loadHead(A.slice(32))
  // console.info('Loaded A from repo')
  // hA.inspect()
  // console.info('Expected A equal')
  // fA.inspect()

  const hB = await repo.loadHead(B.slice(32))
  // console.info('Loaded B from repo')
  // hB.inspect()
  // console.info('Expected B equal')
  // fB.inspect()

  t.equal(hA.last.sig.hexSlice(), fA.last.sig.hexSlice(), 'A loaded successfully')
  t.equal(hB.last.sig.hexSlice(), fB.last.sig.hexSlice(), 'B loaded successfully')

  const lA = await repo.loadLatest(A.slice(32))
  const lB = await repo.loadLatest(B.slice(32))
  t.equal(lA.last.sig.hexSlice(), fD.get(-2).sig.hexSlice(), 'A lastWrite successfully')
  t.equal(lB.last.sig.hexSlice(), fB.get(-2).sig.hexSlice(), 'B lastWrite successfully')
})

test('repo.rollback(head, ptr)', async t => {
  /* The problem: (top down)
   *   A0  B0~
   *    |  B2 <-- current B / after rollback latest B
   *   A1  <-- after rollback latest-A
   *   B1~ <-- after rollback new A-head
   * ! A2
   * ! A3 <-- latest
   * ! B3 <-- latest B /current A
   *
   * await repo.rollback(A, B1)
   */
  const repo = new Repo(DB(), async () => true)
  const [A, B] = Array.from(new Array(2)).map(() => Feed.signPair().sk)
  const fA = new Feed()
  const fB = new Feed()
  fA.append('A0', A)
  fA.append('A1', A)
  // const A1 = fA.last
  await repo.merge(fA)
  fB.append('B0', B)
  await repo.merge(fB)
  fA.append('B1', B)
  const B1 = fA.last
  fA.append('A2', A)
  fA.append('A3', A)
  const A3 = fA.last
  await repo.merge(fA)
  fB.append('B2', B)
  const B2 = fB.last
  await repo.merge(fB)
  fA.append('B3', B)
  const B3 = fA.last
  await repo.merge(fA)

  let currentA = await repo.headOf(A.slice(32))
  let latestA = await repo.latestOf(A.slice(32))
  let currentB = await repo.headOf(B.slice(32))
  let latestB = await repo.latestOf(B.slice(32))
  // console.log('Feed A')
  // fA.inspect()
  // console.log('Feed B')
  // fB.inspect()

  hexCmp(latestB, B3.sig, 'Latest B ptr equals B3')
  hexCmp(currentB, B2.sig, 'Head B ptr equals B2')
  hexCmp(latestA, A3.sig, 'Latest A ptr equals A3')
  hexCmp(currentA, B3.sig, 'Head A ptr equals A3')

  const evicted = await repo.rollback(A.slice(32), B1.sig)
  t.equal(evicted.length, 3)
  // hexCmp(evicted.first.sig, A2.sig)
  // hexCmp(evicted.last.sig, B3.sig)

  currentA = await repo.headOf(A.slice(32))
  latestA = await repo.latestOf(A.slice(32))
  currentB = await repo.headOf(B.slice(32))
  latestB = await repo.latestOf(B.slice(32))

  hexCmp(currentA, B1.sig, 'Head A ptr equals B1')
  hexCmp(currentB, B2.sig, 'Head B ptr equals B2')

  t.notOk(latestA, 'tag A deleted')
  t.notOk(latestB, 'tag B deleted')
  // TODO: latest-tags are deleted for now.
  // hexCmp(latestB, B2.sig, 'Latest B ptr equals B2')
  // hexCmp(latestA, A1.sig, 'Latest A ptr equals A1')
  function hexCmp (a, b, desc) {
    return t.equal(a?.hexSlice(0, 4), b?.hexSlice(0, 4), desc)
  }
})
