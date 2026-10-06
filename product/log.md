# History

What changed in the product bundle and why, newest first. A commit's diff shows the change; this file keeps the reason.

## 2026-10-06

- **Update**: [the feature map](FEATURES.md) records that `DataIngestor` is deprecated: it warns on construction and goes in 0.15.0, since no use case and no code in the package calls it.
- **Update**: every persona, use case, and metric id named outside a heading, in the bundle and on the roadmap, now links to its card, and the bundle test holds each page to that. The roadmap also names the issue that tracks each of its sections, so a reader reaches the card, the issue, or the metric from the item in one click.
- **Update**: [DR-02](metrics/DR-02.md) moves from 3 of 4 to 4 of 4. A `.parquet` path read as CSV by default now fails naming the file, the format it was read as, and that `format` is not set, where it named only the missing key. The fix closes #133, the first of track B.
- **Creation**: [the feature map](FEATURES.md), which puts every shipped feature and every roadmap item against the use case it serves, with the ones that serve none justified. The roadmap now names a use case on each item, the pull request template asks every change for one, and the GitLab template is frozen by a decision record, since no user has named GitLab.
- **Update**: [who Veridelta serves](USERS.md) now quotes the maintainer's six answers, holds four personas and two anti-personas with their evidence and unknowns, five use cases, and a cut list. [The key metrics](KEY_METRICS.md) gain three drivers and three guardrails, each measured with its command. The north star [NS-01](metrics/NS-01.md) becomes the steps to a correct verdict on the quick start, by the maintainer's choice, since the one known user is a newcomer with two CSV files. Parity across backends moves to the guardrail [GR-01](metrics/GR-01.md), and the Live Warehouses run still comes before 0.15.0.
- **Creation**: [who Veridelta serves](USERS.md), with the first persona and use case; [the key metrics](KEY_METRICS.md), with the north star and one driver; and the test that holds the bundle to its format and its trace.
