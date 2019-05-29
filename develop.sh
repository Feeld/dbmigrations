#!/usr/bin/env bash
exec ghcid --command='cabal new-repl --ghc-options="-j -Wwarn +RTS -N2 -A128m -qn1 -RTS"' --warnings "$@"
