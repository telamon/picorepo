import { b2h, toU8 } from 'picofeed'
const C_BG = '#2f383e' // graphite
const C_BG2 = '#2c323d' // slate
const C_FG = '#bbbba9' // cream
const C_FG2 = '#818e85' // subtle
const C_BGH = '#cd94b0' // violet
const C_BGC = '#db797b' // rust
const C_BGL = '#9ab278' // leaf
const C_BGO = '#73a8a2' // sky/sea
const C_P = '#d08c6d' // copper
// const C_Y = '#cbaf78' // ochre

export async function inspect (repo, opts = {}) {
  const heads = await repo.listHeads()
  const tails = await repo.listTails()
  const latest = await repo.listLatest()
  const feeds = await repo.listFeeds()
  const feedHeads = await repo.listFeedHeads()

  const colors = opts.colors || {}
  const makeBlockLabel = opts.blockLabel || (block => {
    const body = b2h(block.body, 8)
    const id = hx8(block.sig)
    const author = hx8(block.key)
    return `ID: ${id}\nKEY: ${author}\nBDY: ${body}`
  })

  // generate dot-graph
  let nodes = '// Nodes\n'
  let edges = '// Edges\n'
  const chains = []
  const orphaned = []
  // Generate head nodes
  for (const h of heads) {
    const id = hx8(h.key)
    const label = `@author HEAD\n${hx8(h.key)}`
    nodes += `"H${id}"
      [
        fontcolor="${C_BGH}",
        color="${C_BGH}",
        label="${label}"
      ];\n`
    edges += `"H${id}" -> "B${hx8(h.value)}"[arrowhead=odot];\n` // link to tag to block
    chains.push(h.value)
  }

  for (const t of tails) {
    const id = hx8(t.value)
    const label = `@author TAIL\n ${hx8(t.key)}`
    nodes += `"T${id}"
      [
        fontcolor="${C_BGH}",
        color="${C_BGH}",
        label="${label}"
      ];\n`
    // edges += `"T${id}" -> "B${id}";\n` // link to tag to block
    edges += `"B${id}" -> "T${id}"[dir=back,arrowtail=odot];\n` // genesis links to tail for visual clarity.
  }

  for (const l of latest) {
    const id = hx8(l.value)
    const label = `@latest\n ${hx8(l.key)}`
    nodes += `"L${id}"
      [
        fontcolor="${C_BGL}",
        color="${C_BGL}",
        label="${label}"
      ];\n`
    edges += `"L${id}" -> "B${id}"[arrowhead=odot];\n` // link to tag to block
  }

  for (const f of feeds) {
    const id = hx8(f.key)
    const label = `@chain TAIL\n${hx8(f.value)}`
    nodes += `"F${id}"
      [
        fontcolor="${C_BGC}",
        color="${C_BGC}",
        label="${label}"
      ];\n`
    // edges += `"F${id}" -> "B${id}"[dir=back];\n` // link to tag to block
    edges += `"B${hx8(f.value)}" -> "F${id}"[dir=back,arrowtail=odot];\n` // link to tag to block
    if (!chains.find(c => c.equals(f.key))) {
      chains.push(f.key)
      orphaned.push(f.key)
    }
  }

  for (const f of feedHeads) {
    const id = hx8(f.key)
    const label = `@chain HEAD\n${hx8(f.key)}`
    nodes += `"FT${id}"
      [
        fontcolor="${C_BGC}",
        color="${C_BGC}",
        label="${label}"
      ];\n`
    edges += `"FT${id}" -> "B${hx8(f.value)}"[arrowhead=odot];\n` // link to tag to block
  }

  let nChains = 0
  let nBlocks = 0
  let nBytes = 0
  let nBytesContent = 0

  for (const ptr of chains) {
    const isOrphan = orphaned.find(c => c.equals(ptr))
    nChains++
    // Chainload blocks
    for await (const block of repo._chainLoad(ptr)) {
      nBlocks++
      nBytes += block.buffer.length
      nBytesContent += block.size

      const id = hx8(block.sig)
      const label = makeBlockLabel(block, {
        link (blockId, dotOpts = `style=dashed,weight=0.1,color="${C_P}"`) {
          blockId = toU8(blockId)
          // if (!Buffer.isBuffer(blockId)) throw new Error('link(id) expected id to be Buffer')
          edges += `"B${id}" -> "B${hx8(blockId)}"[${dotOpts}];\n` // Link to parent
        }
      })
      const c = !isOrphan
        ? colors.block || C_FG
        : colors.orphanBlock || C_BGO
      nodes += `"B${id}"
        [
          fontcolor="${c}",
          color="${c}"
          shape="square",
          label="${label}"
        ];\n`
      if (!block.genesis) {
        edges += `"B${id}" -> "B${hx8(block.psig)}"
          [
            color="${C_FG2}",
            fillcolor="${C_FG}",
            weight=1.0
          ];\n` // Link to parent
      }
    }
  }
  const label = opts.label ||
    `[PicoRepo] Authors: ${latest.length} Feeds: ${nChains}, Blocks: ${nBlocks}, Bytes: ${bs(nBytes)} (${bs(nBytes - nBytesContent)} ${(100 * (nBytes - nBytesContent) / nBytes).toFixed(2)}%)`
  const graph = `
    digraph G {
      graph [pad=0.2,labelloc="t",fontcolor="${C_FG}",bgcolor="${C_BG}",fontname="fixed",overlap="false",center="1",ratio="compress",label="${label}",rankdir=BT];
      edge [color="black",fillcolor="${C_BG2}",weight=2.0];
      node [style=filled,fillcolor="${C_BG2}",color="${C_FG2}",shape="rect",fontcolor="${C_FG}",shape=box];
    ${nodes}
    ${edges}
    }
  `
  return graph

  function hx8 (buf) {
    return b2h(buf.subarray(0, 4))
  }
}

// TODO: remove, causes more issue with build
// tools than intended
/*
async function dump (repo, filename = 'repo.dot', opts = {}) {
  const dot = await inspect(repo, opts)
  require('fs').writeFileSync(filename, dot)
}
*/

// Borrowed from https://github.com/mafintosh/tiny-byte-size/blob/master/index.js
export function bs (bytes, space = false) {
  const b = bytes < 0 ? -bytes : bytes
  if (b < 1e3) return fmt(bytes, 'B')
  if (b < 1e6) return fmt(bytes / 1e3, 'kB')
  if (b < 1e9) return fmt(bytes / 1e6, 'MB')
  return fmt(bytes / 1e9, 'GB')
  function fmt (n, u) {
    n = Math.round(10 * n) / 10
    return n + (space ? ' ' : '') + u
  }
}
