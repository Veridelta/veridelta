// Accessibility fixes over the Material theme, for parts its own script builds.
// `document$` is the theme's page-load stream, which runs after it mounts each page.

// A keyboard scrolls a wide block only once the block can take focus. The theme
// does this for its own code blocks, but not for tables on a narrow screen, the
// tutorials' notebook cells, or a diagram's source when Mermaid cannot load.
function focusWideBlocks() {
  var blocks = document.querySelectorAll(
    ".md-typeset__scrollwrap, .md-typeset pre > code, " +
      ".jupyter-wrapper .highlight-ipynb, .jupyter-wrapper .highlight > pre"
  );
  blocks.forEach(function (block) {
    if (block.matches(".highlight pre > code")) {
      return;
    }
    if (block.scrollWidth > block.clientWidth) {
      block.setAttribute("tabindex", "0");
    } else {
      block.removeAttribute("tabindex");
    }
  });
}

document$.subscribe(function () {
  // The search dialog has no accessible name.
  var search = document.querySelector('.md-search[role="dialog"]');
  if (search) {
    search.setAttribute("aria-label", "Search");
  }
  // Each code block puts its copy button in a nav element, so every block repeats an
  // unnamed navigation landmark. A copy button is not navigation, so the landmark goes.
  document.querySelectorAll(".md-code__nav").forEach(function (nav) {
    nav.setAttribute("role", "none");
  });
  focusWideBlocks();
});

window.addEventListener("resize", focusWideBlocks);
