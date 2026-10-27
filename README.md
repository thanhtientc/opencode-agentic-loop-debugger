# OpenCode Agentic Loop Debugger

A local web app that visualizes selected parts of OpenCode's agentic loop and
can pause at supported V2 plugin hooks for educational purposes.

The packaged Mermaid diagram is used by default. A project-local
`agentic-loop-lifecycle.mmd` can override it.

Mermaid 11.17.2 is bundled and served locally; the debugger does not need a CDN
to render the diagram.

## Install from GitHub

Install the plugin globally from GitHub with OpenCode's CLI:

```sh
opencode plugin add github:thanhtientc/opencode-agentic-loop-debugger
```

This installs the package globally. In OpenCode, start the web app and enable
step mode with:

```text
/agentic-loop-debugger on
```

Use **Next**, **Continue**, and **Pause** in the web app. Turn the debugger off
and stop its local web server with:

```text
/agentic-loop-debugger off
```

The app binds to `127.0.0.1:45999` by default. Override the port with
`AGENTIC_LOOP_PORT`.

## Event-to-node mapping

| Diagram node | OpenCode hook/event signal(s) |
| --- | --- |
| **Session starts** | `session.created` |
| **Wait for prompt** | `session.idle`; `session.status` (`idle`); final `session.step.streamed` |
| **Handle input** | `session.hook("prompt")` |
| **Build initial context** | First `session.hook("context")` per turn |
| **Start task** | Initial `context` hook · synthetic checkpoint |
| **Load Agent Skill** (prompt-provided) | `prompt` hook · `prompt.skills` · synthetic checkpoint |
| **Load Agent Skill** (agent-activated) | `session.skill.activated`; next `context` hook; skill `tool.execute.before` |
| **LLM Core** | Primary `http.request` / `http.response`; continuation `context` |
| **Choose tool** | `tool.execute.before` |
| **Use tool** | `tool.execution` · synthetic signal; `session.tool.progress` |
| **Result** | `tool.execute.after`; conditional `session.step.streamed` |
| **Return response** | `session.text.ended` (no tool in step; visual breakpoint) |
| **Human review** | `permission.hook("evaluate")` (`effect: "ask"`) |

## Events generally not represented

- `session.text.started`
- `session.text.delta`
- `session.reasoning.started`
- `session.reasoning.delta`
- `session.reasoning.ended`
- `session.step.started`
- `session.tool.input.delta`
- `session.tool.input.ended`
- `session.tool.called`
- `session.tool.success`
- `session.tool.failed`

## Develop locally

OpenCode can load this package directory directly by adding its path to the
`plugins` array in `opencode.json(c)`, or the entrypoint can be copied into a
project's `.opencode/plugins/` directory.

The package targets the OpenCode V2 plugin API and pins `@opencode/plugin` to
2.1.4. Use a compatible package version when targeting another OpenCode release.
