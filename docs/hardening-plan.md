# Plan: harden ActiveCluster and prepare it for release

Self-contained brief. It follows `docs/jgroups-validation-plan.md`, which is complete: the
three-tier test suite exists and passes (`mvn test`, `mvn verify -Pmultiprocess`).

## Goals

1. Current toolchain: JGroups 5.5.8.Final, a formatter that runs on JDK 25, current Maven plugins.
2. Remove dead code.
3. Direct messages go point-to-point (JGroups unicast) instead of broadcast-and-drop.
4. Partition and merge resiliency, proven by tests.
5. Release hardening: diagnosable exceptions, input validation, listener isolation, a clean
   javadoc and release build, a module name.
6. Rewrite the README around the architecture with worked examples.

## Constraints

- Public types in `com.github.xgp.cluster` may gain members, but no existing signature changes.
- Never weaken a test to go green. Every behaviour change gets a test that failed first.
- `mvn -q test` green five runs in a row and `mvn -q verify -Pmultiprocess` green at the end.
- Commit after each step as `xgp <244253+xgp@users.noreply.github.com>`, no AI attribution.

## Steps

| # | Step | Verification |
|---|---|---|
| 1 | Bump JGroups to 5.5.8.Final, surefire/failsafe/compiler/jar plugins to current releases. Fix any config attribute or API the new JGroups rejects. | Full suite and multiprocess green. |
| 2 | Upgrade `fmt-maven-plugin` to 2.29 (2.25 crashes on JDK 25) and pin google-java-format 1.37.0 as a plugin dependency. Bind `fmt:check` to `verify`. | `mvn fmt:format` and `fmt:check` run on JDK 25 with formatter 1.37.0. |
| 3 | Dead code: `RequestHandler`/`handle()` in `JGroupsCluster`, the config fallback ladder and its constants, unused fields, methods and constructors. | Compiler plus grep for references; suite green. |
| 4 | Direct messages: `send(nodeDestination)` resolves the member's current address from the node map and sends a unicast `ObjectMessage`. No property is written into the caller's message. Unknown or departed target throws `ClusterException`. Receivers route by `dest != null`. | New messaging tests: no `__direct_to` property, other nodes never receive the bytes (count received JGroups messages), unknown target throws, direct to self works, destination built by name only works. |
| 5 | Partition and merge: `test-partition.xml` = loopback stack plus `DISCARD`. Tests split {1,2} vs {3}, assert each side sees `FAILED_NODE`+`REMOVE_NODE` for the other and elects its own coordinator; heal, assert a `MergeView`, `ADD_NODE` for the returning members, one agreed coordinator, and state changed during the partition visible everywhere. Fix what fails. | `JGroupsPartitionTest`. |
| 6 | Hardening: `ClusterException` message and cause constructors used everywhere; invalid selectors rejected at `createConsumer`; one consumer's or listener's exception never blocks delivery to others; null-argument checks; `GroupModel` group-collection bookkeeping recomputed rather than diffed; `Automatic-Module-Name`; release build (`-Preleases`, signing skipped) produces javadoc without errors. | Unit tests per item; `mvn -Preleases -Drelease.sign.skip=true verify`. |
| 7 | Flakiness pass: ten consecutive `mvn -q test`. | Logged. |
| 8 | README rewrite: concepts, architecture (membership, state, election, messaging, partition handling), configuration, examples, testing, limitations. `docs/architecture.md` for the deeper protocol notes. | Examples compile (they are mirrored in a test). |

## Out of scope

- Publishing (the parent POM's OSSRH deploy target was retired by Sonatype in 2025; moving to the
  Central Portal is the owner's call and lives in `oss-parent`).
- Replacing Java serialization, a full SQL-92 selector, and a CI workflow.
