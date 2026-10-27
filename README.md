# OpenCode Agentic Loop Debugger

A local web debugger that visualizes OpenCode's agentic loop and can pause at
awaited V2 plugin hooks. The packaged Mermaid diagram is used by default; a
project-local `agentic-loop-lifecycle.mmd` can override it.

## Install from GitHub

After this repository is pushed to GitHub:

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

## Develop locally

OpenCode can load this package directory directly by adding its path to the
`plugins` array in `opencode.json(c)`, or the entrypoint can be copied into a
project's `.opencode/plugins/` directory.

The package targets the OpenCode V2 plugin API and pins `@opencode/plugin` to
2.1.4. Use a compatible package version when targeting another OpenCode release.
