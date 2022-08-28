async function inspect (repo, opts = {}) {
  const heads = await repo.listHeads()
  const tails = await repo.listTails()
  const latest = await repo.listLatest()
  const feeds = await repo.listFeeds()
  const feedHeads = await repo.listFeedHeads()

  const colors = opts.colors || {}
  const makeBlockLabel = opts.blockLabel || (block => {
    const body = block.body.subarray(0, 8).toString('utf8')
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
    const label = `Head \n${hx8(h.key)}`
    const c = colors.head || 'lightblue3'
    nodes += `"H${id}"[fillcolor=${c},label="${label}"];\n`
    edges += `"H${id}" -> "B${hx8(h.value)}";\n` // link to tag to block
    chains.push(h.value)
  }

  for (const t of tails) {
    const id = hx8(t.value)
    const label = `Tail\n ${hx8(t.key)}`
    const c = colors.tail || 'sienna'
    nodes += `"T${id}"[fillcolor=${c},label="${label}",shape=box];\n`
    // edges += `"T${id}" -> "B${id}";\n` // link to tag to block
    edges += `"B${id}" -> "T${id}"[dir=back];\n` // genesis links to tail for visual clarity.
  }

  for (const l of latest) {
    const id = hx8(l.value)
    const label = `Latest\n ${hx8(l.key)}`
    const c = colors.latest || 'seagreen'
    nodes += `"L${id}"[fillcolor=${c},label="${label}",shape=box];\n`
    edges += `"L${id}" -> "B${id}";\n` // link to tag to block
  }

  for (const f of feeds) {
    const id = hx8(f.key)
    const label = `FeedTail\n${hx8(f.value)}`
    const c = colors.feed || 'mediumvioletred'
    nodes += `"F${id}"[fillcolor=${c},label="${label}",shape=box];\n`
    edges += `"F${id}" -> "B${id}"[dir=back];\n` // link to tag to block
    if (!chains.find(c => c.equals(f.key))) {
      chains.push(f.key)
      orphaned.push(f.key)
    }
  }

  for (const f of feedHeads) {
    const id = hx8(f.key)
    const label = `FeedHead\n${hx8(f.key)}`
    const c = colors.feed || 'mediumvioletred'
    nodes += `"FT${id}"[fillcolor=${c},label="${label}",shape=box];\n`
    edges += `"FT${id}" -> "B${hx8(f.value)}";\n` // link to tag to block
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
      const label = makeBlockLabel(block)
      const c = !isOrphan
        ? colors.block || 'seashell'
        : colors.orphanBlock || 'gray80'
      nodes += `"B${id}"[fillcolor=${c},label="${label}",shape="square"];\n`
      if (!block.isGenesis) {
        edges += `"B${id}" -> "B${hx8(block.parentSig)}";\n` // Link to parent
      }
    }
  }
  const label = opts.label ||
    `[PicoRepo] Authors: ${latest.length} Feeds: ${nChains}, Blocks: ${nBlocks}, Bytes: ${nBytes} (${nBytesContent})`
  const graph = `
    digraph G {
      graph [fontname="fixed",overlap="false",center="1",ratio="compress",label="${label}",rankdir=BT];
      node [style=filled,fillcolor=white,shape="circle"];
    ${nodes}
    ${edges}
    }
  `
  return graph

  function hx8 (buf) {
    return buf.subarray(0, 4).toString('hex')
  }
}

async function dump (repo, filename = 'repo.dot', opts = {}) {
  const dot = await inspect(repo, opts)
  require('fs').writeFileSync(filename, dot)
}

module.exports = {
  inspect,
  dump
}
