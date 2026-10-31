{
  description = "hourly-changefeed: versioned job postings from public ATS boards, with hourly change files";

  inputs.nixpkgs.url = "github:NixOS/nixpkgs/nixos-25.05";

  outputs = { self, nixpkgs }:
    let
      systems = [ "x86_64-linux" "aarch64-linux" ];
      forAll = f: nixpkgs.lib.genAttrs systems (s: f nixpkgs.legacyPackages.${s});
    in {
      devShells = forAll (pkgs: {
        default = pkgs.mkShell {
          packages = [ pkgs.postgresql_17 pkgs.python312 pkgs.uv ];
          shellHook = ''
            export PGDATA="$PWD/.pgdata"
            export PGHOST="$PGDATA/sock"
            export PGDATABASE=postgres
            export UV_PYTHON_DOWNLOADS=never
            export UV_PYTHON=${pkgs.python312}/bin/python3
          '';
        };
      });

      packages = forAll (pkgs: {
        default = pkgs.python312Packages.buildPythonApplication {
          pname = "changefeed";
          version = "0.1.0";
          pyproject = true;
          src = ./.;
          build-system = [ pkgs.python312Packages.hatchling ];
          dependencies = with pkgs.python312Packages; [ psycopg httpx ];
        };
      });
    };
}
