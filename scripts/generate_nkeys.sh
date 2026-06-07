#!/usr/bin/env bash
#
# Generate NKey pairs for the iot-mesh-cluster NATS deployment.
#
# Run once during cluster setup. Prints two YAML blocks for paste into:
#   - ansible/group_vars/all/vars.yml   (public keys)
#   - ansible/group_vars/all/vault.yml  (private seeds, then ansible-vault encrypt)
#
# Requires `nk` from the NATS NKeys project:
#   go install github.com/nats-io/nkeys/nk@latest

set -euo pipefail

if ! command -v nk &>/dev/null; then
    echo "Error: 'nk' not found." >&2
    echo "Install with: go install github.com/nats-io/nkeys/nk@latest" >&2
    exit 1
fi

HOSTS=(viscous wave)
SERVICES=(sensehat mmwave intercom logic voice)

nkey_pair() {
    local seed pub
    seed="$(nk -gen user)"
    pub="$(echo "$seed" | nk -inkey /dev/stdin -pubout)"
    printf '%s\t%s\n' "$pub" "$seed"
}

declare -A SVC_PUB SVC_SEED

echo "Generating NKey pairs..." >&2
for host in "${HOSTS[@]}"; do
    for svc in "${SERVICES[@]}"; do
        IFS=$'\t' read -r pub seed < <(nkey_pair)
        SVC_PUB["$host:$svc"]="$pub"
        SVC_SEED["$host:$svc"]="$seed"
        printf "  %-8s %-9s %s\n" "$host" "$svc" "$pub" >&2
    done
done

CLUSTER_TOKEN="$(openssl rand -hex 32)"
echo "  cluster   token     $(echo "$CLUSTER_TOKEN" | head -c 12)..." >&2

cat <<'EOF'

# ============================================================
# PASTE INTO: ansible/group_vars/all/vars.yml
# ============================================================
EOF

echo "nats_users:"
for svc in "${SERVICES[@]}"; do
    echo "  ${svc}:"
    for host in "${HOSTS[@]}"; do
        echo "    ${host}: \"${SVC_PUB[$host:$svc]}\""
    done
done

cat <<'EOF'

# ============================================================
# PASTE INTO: ansible/group_vars/all/vault.yml
# Then encrypt: ansible-vault encrypt ansible/group_vars/all/vault.yml
# ============================================================
EOF

echo "vault_nats_seeds:"
for host in "${HOSTS[@]}"; do
    echo "  ${host}:"
    for svc in "${SERVICES[@]}"; do
        echo "    ${svc}: \"${SVC_SEED[$host:$svc]}\""
    done
done

echo ""
echo "vault_nats_cluster_token: \"${CLUSTER_TOKEN}\""
