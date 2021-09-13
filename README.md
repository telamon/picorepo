[`pure | 📦`](https://github.com/telamon/create-pure)
[`code style | standard`](https://standardjs.com/)
# picorepo

> Persistent efficient binary block storage for picofeeds

This is a low-level blockstore for [picofeeds](https://github.com/telamon/picofeed/) that stores consistent chains using a fast access scheme.
Uses a levelup interface for storage abstraction, works in nodejs (level-db) and in browser (IndexedDB) so besides
adding this modules as a dependency you have to additionally add [levelup](https://github.com/Level/levelup) as a peer dep.
Use `memdown` for "in memory" unit-tests.

## Use

```bash
$ npm install picorepo levelup memdown
```

```js
const PicoRepo = require('picorepo')
const PicoFeed = require('picofeed')
const database = levelup(memdown()) // Or leveljs/IndexedDB, read "levelup" docs

const repo = new PicoRepo(database)

// Generate an crypto identity consisting of a public and secret key
const { pk, sk } = Feed.signPair()

// Example dummy feed
const chain = new PicoFeed()
chain.append('Hello', sk)
chain.append('World', sk)
chain.append('Of', sk)
chain.append('Blockchains', sk)
chain.append('And', sk)
chain.append('Beliefsystems', sk)
chain.inspect() // console-logs contents

// Persist feed to cold-storage
const numberAccepted = await repo.merge(chain) // => 6 (blocks)

// Retrieve the feed using the public-key
const restored = await repo.loadHead(pk)
restored.inspect() // => logs same contents
```

## Donations

```ad
 _____                      _   _           _
|  __ \   Help Wanted!     | | | |         | |
| |  | | ___  ___ ___ _ __ | |_| |     __ _| |__  ___   ___  ___
| |  | |/ _ \/ __/ _ \ '_ \| __| |    / _` | '_ \/ __| / __|/ _ \
| |__| |  __/ (_|  __/ | | | |_| |___| (_| | |_) \__ \_\__ \  __/
|_____/ \___|\___\___|_| |_|\__|______\__,_|_.__/|___(_)___/\___|
Discord: https://discord.gg/K5XjmZx

If you're reading this it means that the docs are missing or in a bad state.

Writing and maintaining friendly and useful documentation takes
effort and time. In order to do faster releases
I will from now on provide documentation relational to project activity.

  __How_to_Help____________________________________.
 |                                                 |
 |  - Open an issue if you have questions! :)      |
 |  - Star this repo if you found it interesting   |
 |  - Fork off & help document <3                  |
 |.________________________________________________|
```


## Changelog

### 1.0.0 first release

## Contributing

By making a pull request, you agree to release your modifications under
the license stated in the next section.

Only changesets by human contributors will be accepted.

## License

[AGPL-3.0-or-later](./LICENSE)

2021 &#x1f12f; Tony Ivanov
