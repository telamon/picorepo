// SPDX-License-Identifier: AGPL-3.0-or-later
const test = require('tape')
const Feed = require('picofeed')
const { MemoryLevel } = require('memory-level')
const Repo = require('.')
// const { dump } = require('./dot')
const DB = () => new MemoryLevel({
  keyEncoding: 'buffer',
  valueEncoding: 'buffer'
})

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

  const hB = await repo.loadHead(B.slice(32))

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

test('Each head has a tag referencing the genesis', async t => {
  const repo = new Repo(DB())
  const { pk, sk } = Feed.signPair()
  const feed = new Feed()

  feed.append('0: Hello', sk)
  await repo.merge(feed)
  let feeds = await repo.listFeeds()
  t.equal(feeds.length, 1, 'One unique feed')
  hexCmp(feeds[0].key, feeds[0].value, 'Genesis references itself')
  hexCmp(feed.first.sig, feeds[0].value, 'ChainId equals genesis sig')

  feed.append('1: world', sk)
  feed.append('2: of', sk)
  feed.append('3: DAGs', sk)
  await repo.merge(feed)

  feeds = await repo.listFeeds()
  t.equal(feeds.length, 1, 'One unique feed')

  hexCmp(feeds[0].key, feed.last.sig, 'key equals last block signature')
  hexCmp(feeds[0].value, feed.first.sig, 'chainId is inherited')

  await repo.rollback(pk, feed.get(-2).sig)

  feeds = await repo.listFeeds()
  t.equal(feeds.length, 1, 'One unique feed')

  hexCmp(feeds[0].key, feed.get(-2).sig, 'tag was rolled back')
  hexCmp(feeds[0].value, feed.first.sig, 'chainId is inherited')

  function hexCmp (a, b, desc) {
    return t.equal(a?.hexSlice(0, 4), b?.hexSlice(0, 4), desc)
  }
})

test('Experimental: author can create multiple feeds', async t => {
  const repo = new Repo(DB())
  repo.allowDetached = true
  const { sk } = Feed.signPair()
  const feedA = new Feed()
  feedA.append('A0 Hello', sk)
  feedA.append('A1 World', sk)
  await repo.merge(feedA)

  const feedB = new Feed()
  feedB.append('A2 Cyborg', sk)
  feedB.append('A3 Cool', sk)

  let written = await repo.merge(feedB)
  t.equal(written, 2, 'second feed persisted')

  feedA.append('A4: of', sk)
  feedA.append('A5: Hackers', sk)

  written = await repo.merge(feedA)
  t.equal(written, 2, 'first feed updated')

  // TODO: don't overwrite tail tag if exists.. or what?
  // const tail = await repo.tailOf(pk)
  // t.equals(tail.hexSlice(), feedA.first.sig.hexSlice(), 'Tail not moved')
})

test.only('Experimental: detached mode supports rollback()', async t => {
  const repo = new Repo(DB())
  repo.allowDetached = true
  const { sk } = Feed.signPair()
  const feedA = new Feed()
  feedA.append('A0 Hello', sk)
  feedA.append('A1 World', sk)
  await repo.merge(feedA)

  const feedB = new Feed()
  feedB.append('A2 Cyborg', sk)
  feedB.append('A3 Cool', sk)

  let written = await repo.merge(feedB)
  t.equal(written, 2, 'second feed persisted')

  feedA.append('A4: of', sk)
  feedA.append('A5: Hackers', sk)

  written = await repo.merge(feedA)
  t.equal(written, 2, 'first feed updated')
  // Rollback uses CHAIN_ID when detached active
  const chainA = feedA.first.sig
  const chainB = feedB.first.sig
  // Partial rollback
  const blockTwo = feedA.get(1)
  await repo.rollback(chainA, blockTwo.sig)

  const outA = await repo.resolveFeed(feedA.first.sig)
  t.equal(outA.last.body.toString(), 'A1 World')

  // Full feed removal
  await repo.rollback(chainB)
  try {
    await repo.resolveFeed(feedB.first.sig)
    t.fail('Expected FeedNotFound error')
  } catch (e) { t.equal(e.message, 'FeedNotFound') }

  /* await require('./dot').dump(repo, 'test.dot', {
    blockLabel: b => `${b.sig.hexSlice(0, 6)}\n${b.body.toString()}`
  }) */
})

test('repo.resolveFeed(sig) returns entire feed', async t => {
  const repo = new Repo(DB())
  const { pk, sk } = Feed.signPair()
  const feedA = new Feed()
  feedA.append('0', sk)
  feedA.append('1', sk)
  feedA.append('2', sk)
  feedA.append('3', sk)
  feedA.append('4', sk)
  feedA.append('5', sk)
  feedA.append('6', sk)
  feedA.append('7', sk)
  await repo.merge(feedA)
  let f = await repo.resolveFeed(feedA.get(2).sig)
  t.equal(f.last.body.toString(), '7')

  // await repo.rollback(f.last.sig, f.get(-2).sig)
  await repo.rollback(pk, f.get(-3).sig)
  f = await repo.resolveFeed(feedA.get(2).sig)
  t.equal(f.last.body.toString(), '5')
  // await require('./dot').dump(repo, 'test.dot')
  await repo.rollback(pk)
  t.equal((await repo.listFeeds()).length, 0, 'Empty repo')
})

test('resolveFeed() fast-tracks tip-pointers correctly', async t => {
  const repo = new Repo(DB())
  const { sk } = Feed.signPair()
  const f = new Feed()
  f.append('0', sk)
  await repo.merge(f)
  f.append('1', sk)
  await repo.merge(f)
  const b = await repo.resolveFeed(f.last.sig)
  t.equal(f.length, b.length, 'Full chain loaded')
})

test('Dot graph should be customizable', async t => {
  const enc = JSON.stringify
  const repo = new Repo(DB())
  repo.allowDetached = true
  const hero = Feed.signPair()
  const monster1 = Feed.signPair()
  const monster2 = Feed.signPair()
  const feedA = new Feed()
  const feedB = new Feed()
  const feedC = new Feed()
  feedA.append(enc({ name: 'Hero', lvl: 255, hp: 5000, seq: 0 }), hero.sk)
  feedB.append(enc({ name: 'Goblin', lvl: 10, hp: 30, seq: 0 }), monster1.sk)
  feedC.append(enc({ name: 'Goblin Mage', lvl: 15, hp: 29, seq: 0 }), monster2.sk)
  feedC.append(enc({ action: 'Cook dinner', seq: 1 }), monster2.sk)
  feedB.append(enc({ action: 'Give food', ref: feedC.last.sig.hexSlice(), seq: 2 }), monster2.sk)
  feedB.append(enc({ action: 'Eat', seq: 1 }), monster1.sk)
  feedC.append(enc({ action: 'Eat', seq: 3 }), monster2.sk)

  feedA.append(enc({ action: 'Walk', seq: 1 }), hero.sk)
  feedA.append(enc({ action: 'Talk', seq: 2 }), hero.sk)
  feedB.append(enc({ action: 'Attack', seq: 3, ref: feedA.last.sig.hexSlice() }), hero.sk)
  feedB.append(enc({ action: 'Die', seq: 2 }), monster1.sk)
  feedC.append(enc({ action: 'Summon', seq: 4 }), monster2.sk)
  const feedS = new Feed()
  feedS.append(enc({ name: 'Skeleton', lvl: 5, hp: 100, seq: 5 }), monster2.sk)
  feedS.append(enc({ action: 'Fear Lv1', seq: 6 }), monster2.sk)
  feedA.append(enc({ action: 'Attack', seq: 7, ref: feedS.last.sig.hexSlice() }), monster2.sk)
  feedA.append(enc({ action: 'Run Away', seq: 4 }), hero.sk)
  feedC.append(enc({ action: 'Celebrate', seq: 8 }), monster2.sk)

  const authors = {
    [hero.pk.hexSlice(0, 3)]: '🤴',
    [monster1.pk.hexSlice(0, 3)]: '🧌',
    [monster2.pk.hexSlice(0, 3)]: '👹'
  }
  await repo.merge(feedA)
  await repo.merge(feedB)
  await repo.merge(feedC)
  await repo.merge(feedS)
  const dot = await require('./dot').inspect(repo, {
    blockLabel (block, builder) {
      const d = JSON.parse(block.body)
      if (d.name) {
        return bq`
          ${authors[block.key.hexSlice(0, 3)]}${d.seq}
          ${d.name}
          LVL${d.lvl}
          HP${d.hp}
        `
      }
      if (d.ref) builder.link(Buffer.from(d.ref, 'hex'))
      return bq`
        ${authors[block.key.hexSlice(0, 3)]}${d.seq}
        action:
        ${d.action}
      `
    }
  })
  t.ok(dot)
  // require('fs').writeFileSync('test.dot', dot)
})

// nice this is a useful hack for multiline strings
function bq (str, ...tokens) {
  str = [...str]
  for (let i = tokens.length; i > 0; i--) str.splice(i, 0, tokens.pop())
  return str.join('').split('\n').map(t => t.trim()).join('\n')
}
