# 15. PKI recipes

Working commands for issuing the key material [5. Access](05-access.md) configures. Everything here
is about *producing two PKCS12 files*; what discas does with them, and how a CA is rotated, is on
that page.

**Two independent PKIs.** The peer mesh and the client port are separate trust domains. They may
share a root or not -- that is your decision, and there is no reason they must.

| | Identity is | Node's key store holds | Node's trust store holds |
|---|---|---|---|
| **Peer** | the URI SAN `discas://<cluster_id>/<node_id>` | this node's peer leaf + key + chain | the **cluster CA** |
| **Client** | the certificate subject **`CN=<clientId>`** | this node's client-port **server** leaf + key | the **client CA** |

The client side needs **no per-client table on the node**: any CA-signed certificate whose CN is a
valid client id authenticates, and the node cross-checks the CN against the claimed id in the
handshake. That is why an mTLS deployment has no client registry to maintain.

**Certificate lifetime is yours.** Long-lived leaves (months to years) are the default expectation;
short automated certs are equally supported. discas never renews a certificate -- an external issuer
does -- and only warns when the loaded leaf is near expiry with nothing renewed. Keep your issuer's
renew-before window at or above your alerting lead time, and a rewritten file affects only **new**
handshakes.

---

## Peer material

### keytool

```bash
PW=changeit
# cluster CA
keytool -genkeypair -alias ca -keyalg EC -groupname secp256r1 \
  -keystore ca.p12 -storetype PKCS12 -storepass "$PW" \
  -dname "CN=payments-eu-ca" -ext bc:c -validity 3650
keytool -exportcert -rfc -alias ca -keystore ca.p12 -storepass "$PW" -file ca.crt

# node leaf (SAN = discas://<cluster>/<node>)
keytool -genkeypair -alias node -keyalg EC -groupname secp256r1 \
  -keystore node.p12 -storetype PKCS12 -storepass "$PW" -dname "CN=eu-1"
keytool -certreq -alias node -keystore node.p12 -storepass "$PW" -file node.csr
keytool -gencert -alias ca -keystore ca.p12 -storepass "$PW" \
  -infile node.csr -outfile node.crt \
  -ext "san=uri:discas://payments-eu/eu-1" -validity 365
keytool -importcert -noprompt -alias ca   -keystore node.p12 -storepass "$PW" -file ca.crt
keytool -importcert -noprompt -alias node -keystore node.p12 -storepass "$PW" -file node.crt

# trust store (CA)
keytool -importcert -noprompt -alias ca -keystore trust.p12 \
  -storetype PKCS12 -storepass "$PW" -file ca.crt
```

### openssl

```bash
# cluster CA
openssl ecparam -name prime256v1 -genkey -noout -out ca.key
openssl req -x509 -new -key ca.key -sha256 -days 3650 -subj "/CN=payments-eu-ca" -out ca.crt

# node leaf with URI SAN
openssl ecparam -name prime256v1 -genkey -noout -out node.key
openssl req -new -key node.key -subj "/CN=eu-1" -out node.csr
printf 'subjectAltName=URI:discas://payments-eu/eu-1\n' > node.ext
openssl x509 -req -in node.csr -CA ca.crt -CAkey ca.key -CAcreateserial \
  -days 365 -sha256 -extfile node.ext -out node.crt

# PKCS12 key store (leaf + key + CA chain)
openssl pkcs12 -export -inkey node.key -in node.crt -certfile ca.crt \
  -name node -passout pass:changeit -out node.p12

# trust store (CA) -- keytool builds the PKCS12 truststore
keytool -importcert -noprompt -alias ca -keystore trust.p12 \
  -storetype PKCS12 -storepass changeit -file ca.crt
```

For this hand-run path prefer a **long-lived leaf**. Short (~24 h) certificates are the
automated-CA pattern below, where an agent renews them and expiry doubles as revocation; behind a
`keytool` cron they buy nothing.

> **Revocation on the mesh is member eviction, not a CRL.** To cut a node off immediately --
> regardless of a still-valid certificate -- do a **constant-`N` seat swap**
> ([3. Cluster](03-cluster.md#evicting-a-compromised-member-now)). Short-lived certs
> (revoke-by-non-renewal) are the complementary, expiry-driven lever. discas consults no CRL and no
> OCSP responder.

---

## Client material

The client leaf carries its identity in the **CN**, with no URI SAN. Mint the node's client-port
*server* leaf the same way as a peer leaf, above.

### keytool

```bash
PW=changeit
# client CA
keytool -genkeypair -alias ca -keyalg EC -groupname secp256r1 \
  -keystore client-ca.p12 -storetype PKCS12 -storepass "$PW" \
  -dname "CN=discas-client-ca" -ext bc:c -validity 3650
keytool -exportcert -rfc -alias ca -keystore client-ca.p12 -storepass "$PW" -file client-ca.crt

# client leaf (identity = CN=<clientId>, e.g. web-1)
keytool -genkeypair -alias client -keyalg EC -groupname secp256r1 \
  -keystore client.p12 -storetype PKCS12 -storepass "$PW" -dname "CN=web-1"
keytool -certreq -alias client -keystore client.p12 -storepass "$PW" -file client.csr
keytool -gencert -alias ca -keystore client-ca.p12 -storepass "$PW" \
  -infile client.csr -outfile client.crt -validity 365
keytool -importcert -noprompt -alias ca     -keystore client.p12 -storepass "$PW" -file client-ca.crt
keytool -importcert -noprompt -alias client -keystore client.p12 -storepass "$PW" -file client.crt

# node trust store (the client CA)
keytool -importcert -noprompt -alias ca -keystore client-ca-trust.p12 \
  -storetype PKCS12 -storepass "$PW" -file client-ca.crt
```

### openssl

```bash
# client CA
openssl ecparam -name prime256v1 -genkey -noout -out client-ca.key
openssl req -x509 -new -key client-ca.key -sha256 -days 3650 \
  -subj "/CN=discas-client-ca" -out client-ca.crt

# client leaf: identity is the CN
openssl ecparam -name prime256v1 -genkey -noout -out client.key
openssl req -new -key client.key -subj "/CN=web-1" -out client.csr
openssl x509 -req -in client.csr -CA client-ca.crt -CAkey client-ca.key -CAcreateserial \
  -days 365 -sha256 -out client.crt

# client key store (leaf + key + CA chain)
openssl pkcs12 -export -inkey client.key -in client.crt -certfile client-ca.crt \
  -name client -passout pass:changeit -out client.p12

# node trust store (the client CA)
keytool -importcert -noprompt -alias ca -keystore client-ca-trust.p12 \
  -storetype PKCS12 -storepass changeit -file client-ca.crt
```

> **Revocation on the client port is policy, not PKI.** To cut a client off immediately, drop its
> token record or its ACL grant, then `POST /reload` -- effective at the next request. For pure
> mTLS, remove it from your issuing pipeline and let a short leaf lapse, or roll the client CA to
> invalidate a compromised leaf now.

---

## Kubernetes -- cert-manager

Let cert-manager mint the stores and renew them; discas reads the mounted Secret.

**Peer** -- the identity is a URI SAN:

```yaml
apiVersion: cert-manager.io/v1
kind: Certificate
metadata:
  name: discas-eu-1
spec:
  secretName: discas-eu-1-tls
  duration: 24h
  renewBefore: 8h                     # keep this at or above your alerting lead time
  privateKey: { algorithm: ECDSA, size: 256 }
  uris:
    - "discas://payments-eu/eu-1"     # the discas identity SAN
  issuerRef:
    name: cluster-ca                  # internal CA ClusterIssuer (self-signed or Vault)
    kind: ClusterIssuer
  keystores:
    pkcs12:                           # emit keystore.p12 + truststore.p12 into the Secret
      create: true
      passwordSecretRef: { name: discas-keystore-pass, key: password }
```

**Client** -- the identity is the CN, so set `commonName` and no URI:

```yaml
apiVersion: cert-manager.io/v1
kind: Certificate
metadata:
  name: discas-client-web-1
spec:
  secretName: discas-client-web-1-tls
  duration: 24h
  renewBefore: 8h
  commonName: web-1                   # the discas clientId
  privateKey: { algorithm: ECDSA, size: 256 }
  issuerRef:
    name: client-ca
    kind: ClusterIssuer
  keystores:
    pkcs12:
      create: true
      passwordSecretRef: { name: discas-client-keystore-pass, key: password }
```

Mount the Secret and point `--tls-keystore` / `--tls-truststore` (or the `--client-tls-*` pair) at
`keystore.p12` and `truststore.p12`. cert-manager rewrites the Secret on renewal, and discas picks
the new material up when the pod is asked to reload -- `POST 127.0.0.1:9600/reload`, from a sidecar
watching the mount or from whatever drives your renewals. The node's client-CA trust store is a
single rarely-changing object -- mount the CA once.

Recommended: `duration: 24h`, `renewBefore: 8h`.

> **Addresses on Kubernetes.** Pod addresses change on every reschedule, but discas identifies
> members by `node_id` -- the SAN and the member list -- not by address. Run the cluster as a
> **StatefulSet** with a **headless Service** and let stable per-pod DNS carry the address; the
> member file holds names, resolved fresh at each connection attempt. It is not a Deployment behind
> a horizontal autoscaler.

---

## HashiCorp Vault

A PKI mount as the CA, with a role that constrains identities.

**Peer** -- restrict the URI SANs to this cluster:

```bash
vault secrets enable -path=discas-pki pki
vault write discas-pki/roles/node \
  allowed_uri_sans="discas://payments-eu/*" \
  key_type=ec key_bits=256 max_ttl=24h

# issue for a node (CI or sidecar); returns cert, private_key, ca_chain
vault write discas-pki/issue/node common_name=eu-1 \
  uri_sans="discas://payments-eu/eu-1" ttl=24h
```

**Client** -- restrict the CN to valid client ids:

```bash
vault secrets enable -path=discas-client-pki pki
vault write discas-client-pki/roles/client \
  allow_bare_domains=true allowed_domains="web-1,web-2,reporter" \
  key_type=ec key_bits=256 max_ttl=24h

vault write discas-client-pki/issue/client common_name=web-1 ttl=24h
```

Deliver the material to files with **Vault Agent** templating or a small sidecar: render the leaf
and key into a PKCS12 key store and the CA into a trust store -- `openssl pkcs12 -export` plus
`keytool -importcert`, as above -- then point the flags at them. Vault's TTL drives renewal; have
the same hook that renders the files finish with `curl -X POST 127.0.0.1:9600/reload`, which reads
the key store and the trust store together.

Recommended: `ttl 24h`, agent renews at about half-life.

---

## AWS Private CA (ACM PCA)

Issue from an ACM Private CA and run a renewal helper that writes the two files.

```bash
# The CSR must carry the URI SAN discas://payments-eu/eu-1 (add it via the CSR config)
aws acm-pca issue-certificate \
  --certificate-authority-arn "$CA_ARN" \
  --csr fileb://node.csr --signing-algorithm SHA256WITHECDSA \
  --validity Value=1,Type=DAYS
aws acm-pca get-certificate --certificate-authority-arn "$CA_ARN" \
  --certificate-arn "$CERT_ARN" --output text > node.crt
```

A renewal sidecar, or a Lambda plus a deployment, bundles `node.crt` and the key into `node.p12` and
the CA into `trust.p12` as above, writes them to the node's certificate directory, calls
`POST 127.0.0.1:9600/reload`, and loops before expiry. Reload after both files are written, never
between them: one call applies the pair or neither.

Recommended: one-day validity, renewed daily.

---

## Related

- [5. Access](05-access.md) -- what the node does with these files, and how a CA is rotated
- [3. Cluster and members](03-cluster.md) -- the member file the peer SANs must agree with
- Runbooks: [RB-13 rotate the peer CA](13-runbooks.md#rb-13-rotate-the-peer-ca),
  [RB-14 rotate client credentials](13-runbooks.md#rb-14-rotate-client-credentials)
