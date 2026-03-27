# AGENTS.md

## Product goal
- Increase first-time vote completion and next-day revisit rate.
- Preserve the core idea: consensus shogi with one shared move cadence.
- Optimize for clarity for first-time visitors, not only strong shogi players.

## UX rules
- Keep all UI copy in Japanese.
- Prefer plain-language explanations over shogi jargon.
- Surface current turn, last move, deadline, and current top-voted move near the top of the page.
- Preserve direct board-click voting.
- Favor mobile-first layout and readable spacing.
- Prefer small, reversible UI improvements over schema-heavy rewrites.

## Engineering rules
- Reuse existing components, styles, state management, and API patterns.
- Discover and run the repo's actual verification commands before finishing.
- Do not add heavy engine dependencies unless already present or clearly justified.
- Do not break history pages or current vote submission flow.
- Summarize changed files, verification results, and residual risks at the end.

## Copy guidance
- Explain the product as a fun shared experience, not only as a serious shogi tool.
- Use wording that helps beginners join quickly.
- When adding share text, make the current excitement and daily deadline visible.