# Agents

## 1. Plan Mode Default

- Enter plan mode for ANY non-trivial task (3+ steps or architectural decisions)
- If something goes sideways, STOP and re-plan immediately - don't keep pushing
- Use plan mode for verification steps, not just building
- Write detailed specs upfront to reduce ambiguity

## 2. Coding

- Always add code comments to explain behavior but try to keep it terse.
- Always add comments explaining the reason a class exists and how it fits into the wider architecture.
- Aim to keep state to a minimum.
- Always evaluate if code should be shared via a base class or helpers. Do not pull implementation details up into abstract base classes unless the logic is truly identical across all subclasses with no foreseeable divergence. Shared helpers are better than shared template methods when subclasses may need different control flow.
- Always aim for clean separation of concerns. Avoid spaghetti dependencies across classes and modules.

## 3. Docs

- Always write assuming word-wrap.

## 4. Testing

- Design code and the general architecture to be testable
- Never mark a task complete without tests that pass
- Do not write tests just to pass the code, think in terms of good tests.
- Run not only tests, but deploy to local and check logs, demonstrate it runs
- Unless you think the dimster CLI is the problem, use the dimster CLI to build, deploy, run and teardown rather than executing bespoke commands

## 5. Demand Elegance (Balanced)

- For non-trivial changes: pause and ask "is there a more elegant way?"
- If a fix feels hacky: "Knowing everything I know now, implement the elegant solution"
- Skip this for simple, obvious fixes - don't over-engineer
- Challenge your own work before presenting it

## 6. Core Principles

- **Simplicity First**: Make every change as simple as possible. Impact minimal code
- **No Laziness**: Find root causes. No temporary fixes. Senior developer standards
