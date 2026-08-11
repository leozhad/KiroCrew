---
title: "Vibe coding 1.0 to 4.0: blast radius is the gate"
author: zezhexu
created: 2026-08-11
---

# Vibe coding 1.0 to 4.0: blast radius is the gate

A framing that keeps coming up when we talk about where agent-assisted work is
heading:

- **1.0** — you read the code in an IDE and chat with one agent.
- **2.0** — there are too many sessions to hold in your head, and you have
  stopped reading the code. So the sessions go in a list and you switch between
  them. Kiro Crew's interface is here today.
- **3.0** — you stop caring what any individual agent is doing. You give
  guidance and unblock ten or more agents running at once.
- **4.0** — you stop handing out tasks. Agents hold a standing mandate:
  *maintain this service and handle its operations.*

Three things in it are right and underrated: the binding constraint is human
attention rather than model capability; each rung is defined by an abstraction
the human stops manipulating directly, which is the same pattern as assembly to
compiler to library to service; and the endpoint is role-shaped delegation rather
than task-shaped instruction.

The rest of this post is what the framing hides, because the hidden parts are the
engineering.

## "Not caring" is a symptom, not a mechanism

Human attention is currently a load-bearing correctness mechanism. Remove it
without a replacement and you do not get 3.0 — you get unreviewed output at ten
times the rate.

The quantity that actually gates each rung is **the cost of an undetected mistake
times its probability**. That reframing explains something the attention story
cannot: we already tolerate formatters and dependency bots changing code with no
human in the loop. Not because they are intelligent, but because their blast
radius is small. Blast radius does not appear anywhere in the ladder, and it is
the gate.

A second variable is moving underneath all of this: the marginal cost of an
agent-hour is collapsing relative to a human-hour. The rational response is to
**spend cheap compute to buy back scarce attention** — redundant agents,
adversarial review, self-verification. That gives a usable investment rule: judge
a feature by how many human-seconds each token it spends buys back.

## Two axes, not one

- 1.0 to 2.0 is **interface adaptation to throughput**. The human gave away no new
  responsibility; the switcher changed.
- 2.0 to 3.0 to 4.0 gives away **responsibility itself** — decomposition,
  verification, eventually goal-setting.

Putting all four on one line implies uniform progress, and it invites one
specific mistake: building 3.0 as a better list. A grid, a kanban of agents, a
wall of live sessions. That re-runs the 2.0 move at a higher N and hits the same
wall a little later. Adding a dashboard does not change the fact that a human is
still watching agents and still deciding what each one does next.

## What the human and the agents share

Each generation is better described by its shared artifact than by the human's
mood:

| | Shared artifact |
|---|---|
| 1.0 | Code — text. |
| 2.0 | Code and memory — text plus accumulated context. |
| 3.0 | A running environment and the verdict it renders — behavior. |
| 4.0 | The organization: mandates and an escalation graph — social structure. |

Each step moves the shared object closer to reality. That gives 3.0 a sharp
definition:

> **3.0 is where the human stops supplying the correctness judgment and starts
> supplying a goal plus an oracle.**

So the engineering content of 3.0 is not a page. It is **oracles**: real-environment
tests, canaries, service-level checks, property tests, replay against recorded
production traffic, adversarial agents that check each other. Every oracle you
build buys you some number of agents you no longer have to watch. It also makes
progress measurable — the fraction of agent-produced changes whose accept-or-reject
decision was made by an oracle rather than a person.

One layer up, 3.0 and 4.0 turn out to be the same organ at different scopes. 3.0
builds the *verdict* oracle ("is this change good?"); 4.0 builds the *task* oracle
("does something in the world need attention now?"). Both are: read reality
instead of asking the human. This is why operations is the easiest first 4.0
domain — alarms, error budgets and pages are an exogenous, machine-readable
definition of "something is wrong". Feature development has no such oracle, which
is why 4.0 will arrive domain by domain, ordered by oracle availability, and not
as a single release.

## 3.0 is the dangerous rung

Driving automation has the same shape, and its level 3 is the hard one: the human
is nominally out of the loop but is still the fallback, and handoff latency is
what hurts. Vibe coding 3.0 is structurally the same, which predicts two failures
that have nothing to do with model quality:

1. **Rubber-stamping.** At ten or more concurrent agents, a human's realistic
   per-decision budget is seconds. Item-by-item approval degrades into a reflex,
   and review becomes theater.
2. **Context-rebuild collapse.** On the day something is genuinely wrong, the
   human needs twenty minutes to rebuild enough context to judge it, and
   throughput falls over.

The first failure has a specific consequence: **at that scale the approval UI is
not the safety mechanism — policy is.** Enforceable deny rules, blast-radius
limits, the sandbox, a governance ceiling the agent cannot raise. That layer, not
the session list, is the real substrate of 3.0.

The second failure means "you no longer read the code" has to be stated more
carefully: not *by default*, and the cost of reading it when you must has to stay
near zero. So 1.0 is not a superseded generation, it is 3.0's drill-down. Every
escalation must carry a one-click path to the diff, the log, and the session that
produced it. The four rungs are zoom levels of one control hierarchy that coexist,
not versions that replace each other.

## Porting accountability

Why does a person take responsibility for what they did? Mechanically it is not a
feeling. It is a loop with four parts:

1. **Attribution** — the action traces to an identifiable actor.
2. **A record outside the actor's control** — you cannot write your own history.
3. **Future capability is a function of that record** — trust widens, or is
   withdrawn.
4. **The chain terminates in a name that bears the cost.**

None of those four requires a conscience. People are accountable mostly because
of external structure — they can be fired, sued, or shamed; their name is on the
commit; the team saw the outage. Accountability is overwhelmingly institutional
rather than psychological, which is good news: you do not have to build a
conscience, you have to build the institution, and institutions are software.

Kiro Crew already has 1 and 4, and one special case of 2 — the security keystone
is exactly "the agent may not read or write its own ceiling". What is missing is
**3**. Today an agent whose change gets reverted suffers nothing, and nothing in
its next turn reminds it that this happened.

### The engineering payload of accountability is dynamic blast radius

This is where accountability and the gate meet:

> **Permission is a function of a verified track record.**

A new agent may only touch tests. After twenty clean landings it may touch
production code. One undeclared regression pulls it back to read-only with
mandatory human review. That single sentence satisfies parts 2 and 3, is fully
mechanical, and assumes nothing about the agent's inner life.

It also dissolves the rubber-stamp problem. Scarce human attention stops going
into per-action approval and goes into adjusting one agent's trust tier — one
decision governing a hundred actions.

And the substitute for conscience is not simulated emotion; it is **making the
record unavoidable at decision time**. People do not feel guilt while deciding;
they recall an outcome. An agent whose every turn opens with "your last five
changes: three landed, one was reverted for X, one caused an incident" has that
internal model externalized. Not a mind — evidence.

## What does not port

Three honest limits:

- **Stake.** Nothing threatens a model. Deleting an agent is not a deterrent to
  something with no survival drive, and an "incentive economy" built on that
  premise is theater. The only hard mechanism in that family is a cost cap.
- **Learning from consequence across episodes.** Structurally absent without
  weight updates. The prosthetic — a durable record re-injected into the next
  decision — is strictly weaker.
- **Reputation.** Requires a population that acts on your record, which does not
  exist before 4.0.

The second limit has a measurable ceiling, and it is in this repository. Episodic
ranking multiplies similarity by `math.exp(-0.03 * days_old)`
(`src/kiro_crew/vector_memory.py:1562` and `:1651`, on `4506e9c92`), a half-life
of about 23 days; and the score is then rounded to four decimals
(`src/kiro_crew/vector_memory.py:1565`), so past roughly a year of age typical
scores underflow to `0.0000` and sort order is gone. Retrieval benchmarking with
the harness in [#2123](https://github.com/kirodotdev/KiroCrew/pull/2123) measures
turn-level recall far below session-level recall over a 293-day corpus.

Read those two facts together and the conclusion is uncomfortable: an agent's old
mistakes are structurally forgotten exactly when long-horizon accountability would
start to matter. So an accountability record must not be built on semantic
retrieval. It has to be **a mandatory injected field**, not a memory entry we hope
gets recalled.

## Agent-to-agent is two layers

Coordination between agents is really two problems that arrive at different times:

- **Resource arbitration** — who is editing this file, who holds semantic
  ownership of this subsystem. This is a 3.0 problem; it exists the moment ten
  agents share one repository, and it is purely mechanical.
- **Authority delegation and escalation** — who may approve whom, who answers to
  whom. This is 4.0.

The 4.0 layer has one constraint that is easy to get wrong:

> **An agent hierarchy must rest on asymmetric permission, not on an asymmetric
> prompt.**

If agent B is agent A with a different system prompt, escalating to it is theater
that adds latency. Human escalation chains work because authority and
accountability are asymmetric, not because intelligence is. Each level must have
strictly more of something real: permission, context, or a genuinely different
verifier — another model, another evidence source.

The second constraint comes from our own observation. In the first phase of the
perpetual-agent experiment, an agent escalated correctly, respected a
no-repeat-question discipline, and then hung indefinitely because the delivery
channel had silently degraded. The agent's side was flawless. It stalled because
**nothing obliged the human to answer.** Over the same period the agent fabricated
nothing while the observing tooling produced several instrument failures — the
weak link was not the agent.

So accountability has to be written symmetrically: **an escalation carries a
deadline, and an unanswered escalation must either reroute or automatically narrow
the mandate.** Otherwise a standing mandate is not executable.

## Do not rebuild all of human society

The direction is right, but two guardrails matter.

**Do not port problems agents do not have.** Human institutions carry enormous
machinery for scarcity, mortality, deception, self-interest, and coalition
politics. Today's agents have no self-interest to police. Copying reputation
markets, incentives, and death pays the cost of those mechanisms without having
the problem they solve. Build only the mechanism that matches a failure you have
actually observed.

**The one human failure mode that does transfer is responsibility diffusion.** The
more layers an organization has, the harder it is to find who is accountable. An
agent hierarchy reproduces this instantly, and faster — it is a perfect machine
for "I did what upstream told me". So the first invariant of 4.0 is not an
incentive scheme; it is making diffusion structurally impossible: every action
traces to a named agent, every agent's mandate traces to a named human, and no
anonymous segment is permitted in between.

Worth saying plainly: human society did not solve this problem, it merely diluted
responsibility until the result was tolerable. Copy it wholesale and you copy that
too, at machine speed.

## The smallest next step

The concrete gap today is that a perpetual agent writes its own journal. That is a
self-report, not a record, and part 2 of the loop breaks there.

What is missing is small and requires trusting the agent for none of it. Every
input is already derivable from the forge and the gateway's own logs: per agent,
each pull request's outcome (landed, reverted, closed), each issue's disposition,
and the human minutes consumed. Write that to a record the agent cannot modify and
inject it every turn, outside semantic retrieval.

That one artifact delivers part 2, the input to part 3, and the denominator of the
human-seconds-per-token ratio. It is also the precondition for dynamic blast
radius: without a trustworthy record, a trust tier is just another vibe.
