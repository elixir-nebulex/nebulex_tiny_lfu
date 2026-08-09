defmodule NebulexTinyLfu.MixProject do
  use Mix.Project

  @source_url "https://github.com/elixir-nebulex/nebulex_tiny_lfu"
  @version "3.0.0-dev"

  def project do
    [
      app: :nebulex_tiny_lfu,
      version: @version,
      elixir: "~> 1.15",
      aliases: aliases(),
      deps: deps(),

      # Testing
      test_coverage: [tool: ExCoveralls, export: "test-coverage"],
      test_ignore_filters: [~r{test/(shared|support)/.*\.exs}],

      # Dialyzer
      dialyzer: dialyzer(),

      # Usage Rules
      usage_rules: usage_rules(),

      # Hex
      package: package(),
      description: "A TinyLFU cache adapter for Nebulex",

      # Docs
      docs: docs()
    ]
  end

  def cli do
    [
      preferred_envs: [
        coveralls: :test,
        "coveralls.detail": :test,
        "coveralls.post": :test,
        "coveralls.html": :test,
        "coveralls.json": :test,
        "test.ci": :test
      ]
    ]
  end

  defp deps do
    [
      nebulex_dep(),
      {:nimble_options, "~> 0.5 or ~> 1.0"},
      {:tidefall, github: "cabol/tidefall", branch: "main"},
      {:telemetry, "~> 0.4 or ~> 1.0", optional: true},

      # Test & Code Analysis
      {:excoveralls, "~> 0.18", only: :test},
      {:credo, "~> 1.7", only: [:dev, :test], runtime: false},
      {:dialyxir, "~> 1.4", only: [:dev, :test], runtime: false},
      {:sobelow, "~> 0.14", only: [:dev, :test], runtime: false},
      {:stream_data, "~> 1.2", only: :test},
      {:mimic, "~> 2.0", only: :test},

      # Benchmark Test
      {:benchee, "~> 1.5", only: [:dev, :test]},
      {:benchee_html, "~> 1.0", only: [:dev, :test]},

      # Usage Rules
      {:usage_rules, "~> 1.0", only: [:dev]},

      # Docs
      {:ex_doc, "~> 0.40", only: [:dev, :test], runtime: false}
    ]
  end

  defp nebulex_dep do
    if path = System.get_env("NEBULEX_PATH") do
      {:nebulex, path: path}
    else
      {:nebulex, "~> 3.0"}
    end
  end

  defp aliases do
    [
      "nbx.setup": [
        "cmd rm -rf nebulex",
        "cmd git clone --depth 1 --branch main https://github.com/elixir-nebulex/nebulex"
      ],
      "test.ci": [
        "compile --warnings-as-errors",
        "format --check-formatted",
        "credo --strict",
        "coveralls.html",
        "sobelow --exit --skip",
        "dialyzer --format short"
      ],
      "ur.sync": ["usage_rules.sync"]
    ]
  end

  defp package do
    [
      name: :nebulex_tiny_lfu,
      maintainers: ["Carlos Bolanos"],
      licenses: ["MIT"],
      links: %{"GitHub" => @source_url},
      files: ~w(lib .formatter.exs mix.exs README* CHANGELOG* LICENSE*)
    ]
  end

  defp docs do
    [
      main: "Nebulex.Adapters.TinyLFU",
      source_ref: "v#{@version}",
      source_url: @source_url,
      canonical: "https://hexdocs.pm/nebulex_tiny_lfu",
      extras: ["guides/learning/architecture.md"]
    ]
  end

  defp dialyzer do
    [
      plt_add_apps: [:nebulex],
      plt_file: {:no_warn, "priv/plts/" <> plt_file_name()},
      flags: [
        :error_handling,
        :extra_return,
        :no_opaque,
        :no_return
      ]
    ]
  end

  defp plt_file_name do
    "dialyzer-#{Mix.env()}-#{System.version()}-#{System.otp_release()}.plt"
  end

  defp usage_rules do
    [
      # The file to write usage rules into (required for usage_rules syncing)
      file: "AGENTS.md",

      # Rules referenced from AGENTS.md via @-style file refs into deps/.
      # Keeps AGENTS.md compact and lets Claude Code auto-load the canonical
      # rules from the nebulex / usage_rules deps without inlined drift.
      usage_rules: [
        {:nebulex,
         [
           sub_rules: [
             "workflow",
             "nebulex",
             "elixir-style",
             "elixir"
           ],
           link: :at
         ]},
        {:usage_rules, [sub_rules: ["otp"], link: :at]}
      ],

      # Agent skills configuration
      skills: [
        # Auto-build a "use-<pkg>" skill per dependency
        deps: [:nebulex]
      ]
    ]
  end
end
