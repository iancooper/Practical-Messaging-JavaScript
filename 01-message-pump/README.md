<!-- Generated from the canonical exercise source (docs/01-message-pump/README.md) by emit.py, which lives in the practical-messaging-samples working directory alongside the five language repos. Do not edit this copy -- edit the canonical source and re-emit. -->
# Exercise 1 — The Message Pump #

**Start with [PROBE.md](PROBE.md).** It is the exercise. 35 minutes.

```
node receiver.js  # the pump
node sender.js    # sends one order
```

Management console: <http://localhost:15672> (`guest` / `guest`) — keep it open.

| | |
|---|---|
| [`PROBE.md`](PROBE.md) | the exercise: read, predict, break, fix |
| [`SOLUTION.md`](SOLUTION.md) | what the fix is, in prose. Read it after the predictions, not before |
| `simple-messaging/` | the messaging gateway. Given to you, and correct — **except `message-pump.js`**, which lives here and is the exercise |
| `model/` | the domain — an order, a catalogue, and a handler |
| `sender.js`, `receiver.js` | two console apps |

**The pump in `simple-messaging/message-pump.js` is deliberately wrong.** It runs. Do not copy it.
