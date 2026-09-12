<!-- Generated from the canonical exercise source (docs/02-failing-well/README.md) by emit.py, which lives in the practical-messaging-samples working directory alongside the five language repos. Do not edit this copy -- edit the canonical source and re-emit. -->
# Exercise 2 — Failing Well #

**Start with [PROBE.md](PROBE.md).** 40 minutes.

```
node receiver.js  # the pump
node sender.js    # a good order
node sender.js unmappable | poison | flaky | slow | burst 20
```

Management console: <http://localhost:15672> — filter Queues on `failing-well`. There are four.

| | |
|---|---|
| [`PROBE.md`](PROBE.md) | the exercise |
| [`SOLUTION.md`](SOLUTION.md) | what the fix is, in prose. After the predictions, not before |
| `simple-messaging/channel.js` | the topology, drawn in a comment. Read it |
| `simple-messaging/message-pump.js` | **the one file you need to change** |

**This is exercise 1's answer**, so you start level whether or not you finished it. The pump no
longer crashes and no longer loses messages — and it is still wrong.
