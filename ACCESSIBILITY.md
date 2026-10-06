# Accessibility

Veridelta aims to work for everyone who compares data with it, including people who use a screen reader, a keyboard alone, or magnification. This page covers what we aim for, what we have checked, the barriers we know of, and how to report one.

## What it covers

People read four parts of Veridelta:

- the documentation site, built with MkDocs and the Material theme, tutorials included;
- the HTML report that `veridelta run --html` writes;
- the command line's text on stdout and stderr, and its JSON with `--json`;
- the Markdown summary that the GitHub Action and the GitLab template post on a pull or merge request.

## Priorities

- **Target.** We work toward [WCAG 2.2](https://www.w3.org/TR/WCAG22/) Level AA. It guides the work, and it is not a claim of conformance.
- **Plain text on the command line.** The command line prints plain text, with no color, symbol, or animation that carries meaning. The verdict is also an [exit code](https://veridelta.github.io/veridelta/cli/#exit-codes), and `--json` prints every result in a form any tool can read.
- **Words for every verdict.** The HTML report and the Markdown summary say `PASSED` or `FAILED` in words. Color never carries a result alone.
- **Structure.** Pages and reports use headings in order, real tables, and link text that names its target. The diagram on the home page is described in the paragraph after it.
- **Keyboard.** Each control in the HTML report is a native button, reached with Tab and shown with the browser's focus ring. A table wider than the screen takes focus too, so arrow keys scroll it.
- **Without JavaScript.** The HTML report shows every row it holds. Its script only splits long tables into pages of 25 rows.

## How we checked

On 2026-10-06, [axe-core](https://github.com/dequelabs/axe-core) 4.14 checked every page of the documentation site and a sample HTML report. That is 17 pages, all five tutorials and the 404 page among them. It ran the WCAG 2.2 A and AA rules and its own best practices in Chromium. Each page ran in light and dark mode, at desktop and phone widths, and none had a violation. The home page's diagram could not load during that run, so the check saw its source rather than the drawing.

A script also drove the report and the documentation site by keyboard in Chromium:

- Tab reached each button and each wide table, and arrow keys scrolled the table.
- Enter paged a table, and the page status changed once per page.
- Focus stayed on Next at the last page.
- With JavaScript off, the report showed every row.

No person has tested Veridelta with a screen reader, a keyboard alone, or high magnification yet. Automated checks find only part of the barriers a person meets.

## Known limitations

- On the documentation site without JavaScript, a table or code block wider than the screen cannot take keyboard focus. Only a mouse or touch scrolls it.
- The diagram on the home page loads Mermaid from unpkg.com. Where that is blocked, the page shows the diagram's source, and the paragraph after it still describes it.

A run's counts are also in `veridelta run --json` and in the Markdown summary, and its rows in the [discrepancy files](https://veridelta.github.io/veridelta/results/#artifacts) it writes. The user guide, the AI agents page, and the roadmap are also in [`llms-full.txt`](https://veridelta.github.io/veridelta/llms-full.txt), as one plain text file. The page sources are Markdown files and notebooks in the repository's `docs` folder.

## Supported environments

- **Command line:** any terminal on Linux, macOS, or Windows. CI runs the command-line tests on all three.
- **Documentation site and HTML report:** current versions of Chrome, Edge, Firefox, and Safari. Only Chromium has been checked.
- **Assistive technology:** none tested yet. Reports from people who use a screen reader, a magnifier, or voice control help most.

## Reporting a barrier

Open an issue with the [accessibility form](https://github.com/Veridelta/veridelta/issues/new?template=accessibility.yml). Share only what you are comfortable sharing. These details help:

- what you were trying to do, and what happened instead;
- where it happened: a page URL, a report, or the command you ran;
- your operating system, browser or terminal, and assistive technology, with versions.

You never need to disclose a disability or a diagnosis. Ask questions in [Discussions](https://github.com/Veridelta/veridelta/discussions). Report a security vulnerability as the [security policy](https://github.com/Veridelta/veridelta/security/policy) describes, not in an issue.

### Severity

The form asks how much a barrier affects you, and triage confirms or adjusts it:

- **Critical:** you cannot complete a core task, such as reading a report.
- **Serious:** a task is hard, but a workaround exists.
- **Moderate:** a task is slower or works differently each time.
- **Minor:** the barrier has little effect on use.

### How we respond

- We acknowledge each report and treat it as expertise, not as a complaint.
- We explain the next steps, any known limitation behind the barrier, and a workaround where one exists.
- We triage a barrier as a bug, by its severity.
- We may ask you to confirm a fix before we close the issue.

## Contributor expectations

When you change the HTML report or the documentation site:

- check each changed page with an automated tool, such as [axe DevTools](https://www.deque.com/axe/devtools/);
- tab through each new control, which needs a visible focus ring and a name that says what it does;
- announce content that changes in place, such as a page status, through an ARIA live region.

In anything people read:

- put headings in order, name each link's target in its text, and give each image or diagram a text alternative;
- never let color, a symbol, or position carry meaning alone;
- keep the command line plain text, and put anything a script reads in `--json`.

No CI check covers accessibility yet.

## Ownership

The maintainers own accessibility. They triage reports, track the known limitations above, and keep this page current.

Last reviewed: 2026-10-06.

## Feedback

To improve this page or how we work, open an issue or a pull request.
