#!/bin/sh
# Скачивает последнюю официальную сборку Stockfish под ваш процессор в ./engine/stockfish
set -e
cd "$(dirname "$0")"

FLAGS=$(grep -m1 '^flags' /proc/cpuinfo 2>/dev/null || true)
case "$FLAGS" in
  *" avx2 "*)   ARCH=x86-64-avx2 ;;
  *" sse4_1 "*) ARCH=x86-64-sse41-popcnt ;;
  *)            ARCH=x86-64 ;;
esac
if [ "$(uname -m)" != "x86_64" ]; then
  echo "Официальных сборок для $(uname -m) нет. Поставьте пакет: sudo apt install stockfish"
  exit 1
fi

BASE=https://github.com/official-stockfish/Stockfish/releases
FILE=stockfish-ubuntu-$ARCH.tar
TMP=$(mktemp -d)
OK=
# Последний релиз, а если в нём ещё нет сборки — проверенные версии.
for URL in "$BASE/latest/download/$FILE" "$BASE/download/sf_18/$FILE" "$BASE/download/sf_17.1/$FILE"; do
  echo "Пробую $URL"
  if command -v curl >/dev/null; then
    curl -fsSL --retry 2 -o "$TMP/sf.tar" "$URL" && OK=1 && break
  else
    wget -q -O "$TMP/sf.tar" "$URL" && OK=1 && break
  fi
done
[ -n "$OK" ] || { echo "Не удалось скачать Stockfish"; exit 1; }
tar -xf "$TMP/sf.tar" -C "$TMP"
BIN=$(find "$TMP" -type f -name 'stockfish-*' ! -name '*.tar' | head -n1)
[ -n "$BIN" ] || { echo "Не нашёл бинарник в архиве"; exit 1; }

mkdir -p engine
mv "$BIN" engine/stockfish
chmod +x engine/stockfish
rm -rf "$TMP"

echo "Готово: $(printf 'uci\nquit\n' | ./engine/stockfish | grep -m1 'id name')"
