#!/usr/bin/env bash
# Generated from the canonical exercise source (shared/00-setup/prereqs.sh) by emit.py, which lives in the practical-messaging-samples working directory alongside the five language repos. Do not edit this copy -- edit the canonical source and re-emit.
#
# Run this BEFORE the course, on the machine you are bringing, on a network you trust.
#
# It pulls the two broker images and proves they start. That is the part of the day that
# takes twenty minutes on venue wifi and there is no reason to spend the exercise slot on it.
#
#   ./prereqs.sh
#
set -uo pipefail
cd "$(dirname "$0")"

pass=0; fail=0
ok()   { printf '  \033[32mOK\033[0m    %s\n' "$1"; pass=$((pass+1)); }
bad()  { printf '  \033[31mFAIL\033[0m  %s\n' "$1"; fail=$((fail+1)); }
step() { printf '\n\033[1m%s\033[0m\n' "$1"; }

step "1. Tools"
if command -v docker >/dev/null 2>&1; then ok "docker $(docker --version | sed 's/Docker version //;s/,.*//')"
else bad "docker not found -- install Docker Desktop"; fi

# Docker Desktop ships compose as a subcommand; homebrew ships it as its own binary.
# Both work; find whichever you have.
if docker compose version >/dev/null 2>&1;  then DC="docker compose";  ok "docker compose (plugin)"
elif docker-compose version >/dev/null 2>&1; then DC="docker-compose";  ok "docker-compose (standalone)"
else DC="docker compose"; bad "no docker compose found -- install Docker Desktop or 'brew install docker-compose'"; fi

if docker info >/dev/null 2>&1; then ok "docker daemon is running"
else bad "docker daemon is not running -- start Docker Desktop"; fi

if command -v node >/dev/null 2>&1; then
  nodev=$(node -p 'process.versions.node')
  major=${nodev%%.*}
  if [ "${major:-0}" -ge 20 ] 2>/dev/null; then ok "node v$nodev"
  else bad "node v$nodev -- the exercises need Node 20 or later"; fi

  # Exercise 3's Kafka client is a native addon. It ships prebuilt binaries for a fixed list
  # of Node releases; on anything else npm compiles librdkafka from source, which needs a C++
  # toolchain and about ten minutes. It does work -- it is just not something to discover at
  # the venue. Node 22 LTS is what these exercises were written and measured against.
  abi=$(node -p 'process.versions.modules')
  case "$abi" in
    108|115|120|127|131|137) ok "a prebuilt Kafka binary exists for node v$nodev" ;;
    *) bad "no prebuilt Kafka binary for node v$nodev -- npm will build librdkafka from source (a C++ toolchain, ~10 minutes). Node 22 LTS is the tested version" ;;
  esac
else bad "node not found -- install Node 20 or later"; fi

if command -v npm >/dev/null 2>&1; then ok "npm $(npm --version)"
else bad "npm not found -- it ships with Node"; fi

step "2. Ports"
# The exercises need these three free. A port already in use is by far the most common
# setup failure, and Docker's own error for it ("proxy already running") says nothing useful.
busy=""
for spec in "5672:RabbitMQ (AMQP)" "15672:RabbitMQ (management console)" "9092:Kafka"; do
  port=${spec%%:*}; what=${spec#*:}
  if lsof -nP -iTCP:"$port" -sTCP:LISTEN >/dev/null 2>&1; then
    owner=$(docker ps --format '{{.Names}}\t{{.Ports}}' | awk -v p=":$port->" 'index($0,p){print $1; exit}')
    bad "port $port ($what) is already in use${owner:+ by container '$owner'}"
    busy="yes"
  else
    ok "port $port free -- $what"
  fi
done
if [ -n "$busy" ]; then
  printf '\n  Something already holds a port the exercises need. Either stop it:\n'
  printf '      docker ps            # find it\n      docker stop <name>\n'
  printf '  or change the ports in docker-compose.yml and in the two constants the code uses\n'
  printf '  (simple-messaging/channel.js and simple-eventing/stream.js). See README.md.\n'
fi

step "3. Images (this is the slow part -- do it at home)"
if $DC pull; then ok "images pulled"; else bad "could not pull images"; fi

step "4. Brokers start"
started=""
if [ -n "$busy" ]; then
  bad "not starting the brokers -- a port they need is in use (see above)"
elif $DC up -d; then
  ok "compose up"; started="yes"
else
  bad "compose up failed -- see the error above"
fi

if [ -n "$started" ]; then
printf '  waiting for health'
for _ in $(seq 1 60); do
  rmq=$(docker inspect -f '{{.State.Health.Status}}' practical-messaging-rmq   2>/dev/null || echo none)
  kfk=$(docker inspect -f '{{.State.Health.Status}}' practical-messaging-kafka 2>/dev/null || echo none)
  [ "$rmq" = healthy ] && [ "$kfk" = healthy ] && break
  printf '.'; sleep 2
done
printf '\n'
[ "${rmq:-}" = healthy ] && ok "rabbitmq healthy" || bad "rabbitmq not healthy (status: ${rmq:-none})"
[ "${kfk:-}" = healthy ] && ok "kafka healthy"    || bad "kafka not healthy (status: ${kfk:-none})"
fi

step "5. You can reach them"
if curl -fsu guest:guest http://localhost:15672/api/overview >/dev/null 2>&1
then ok "management console on http://localhost:15672 (guest/guest)"
else bad "cannot reach the RMQ management console on port 15672"; fi

if docker exec practical-messaging-kafka /opt/kafka/bin/kafka-broker-api-versions.sh \
     --bootstrap-server localhost:9092 >/dev/null 2>&1
then ok "kafka answering on localhost:9092"
else bad "kafka not answering on port 9092"; fi

step "6. The code runs"
for d in ../01-message-pump ../02-failing-well ../03-streams; do
  if [ -d "$d/node_modules" ]
  then ok "$(basename "$d") has its dependencies"
  else bad "$(basename "$d") has no node_modules -- run 'npm install' in it"; fi
done

# JavaScript has no build step, so the nearest equivalent is: does every module parse?
for d in ../01-message-pump ../02-failing-well ../03-streams; do
  broken=0
  for f in $(find "$d" -name '*.js' -not -path '*/node_modules/*'); do
    node --check "$f" >/dev/null 2>&1 || broken=$((broken+1))
  done
  if [ "$broken" -eq 0 ]
  then ok "$(basename "$d") parses"
  else bad "$(basename "$d") has $broken file(s) that will not parse -- run 'node --check' on them to see why"; fi
done

printf '\n\033[1m%d passed, %d failed\033[0m\n' "$pass" "$fail"
if [ "$fail" -eq 0 ]; then
  printf 'You are ready. Leave the containers up, or run "%s down" -- the volumes persist either way.\n' "$DC"
else
  printf 'Fix the failures above before the course. Bring this output with you if you are stuck.\n'
fi
exit $(( fail > 0 ))
