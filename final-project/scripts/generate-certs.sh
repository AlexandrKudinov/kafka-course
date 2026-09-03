#!/usr/bin/env bash
set -euo pipefail
CERT_DIR="final-project/certs"
PASSWORD="password"
mkdir -p "$CERT_DIR"
rm -f "$CERT_DIR"/*.jks "$CERT_DIR"/*.crt "$CERT_DIR"/*.key "$CERT_DIR"/*.csr "$CERT_DIR"/*.ext "$CERT_DIR"/*_creds

openssl req -x509 -newkey rsa:2048 -nodes -keyout "$CERT_DIR/ca.key" -out "$CERT_DIR/ca.crt" -days 3650 -subj "/CN=Final-Kafka-CA" >/dev/null 2>&1

generate_broker() {
  local name="$1" port="$2"
  cat > "$CERT_DIR/$name.ext" <<EOT
subjectAltName=DNS:$name,DNS:localhost,IP:127.0.0.1
extendedKeyUsage=serverAuth,clientAuth
EOT
  openssl req -new -newkey rsa:2048 -nodes -keyout "$CERT_DIR/$name.key" -out "$CERT_DIR/$name.csr" -subj "/CN=$name" >/dev/null 2>&1
  openssl x509 -req -in "$CERT_DIR/$name.csr" -CA "$CERT_DIR/ca.crt" -CAkey "$CERT_DIR/ca.key" -CAcreateserial \
    -out "$CERT_DIR/$name.crt" -days 3650 -sha256 -extfile "$CERT_DIR/$name.ext" >/dev/null 2>&1
  openssl pkcs12 -export -in "$CERT_DIR/$name.crt" -inkey "$CERT_DIR/$name.key" -certfile "$CERT_DIR/ca.crt" \
    -name "$name" -out "$CERT_DIR/$name.p12" -passout pass:"$PASSWORD" >/dev/null 2>&1
  keytool -importkeystore -noprompt -srckeystore "$CERT_DIR/$name.p12" -srcstoretype PKCS12 -srcstorepass "$PASSWORD" \
    -destkeystore "$CERT_DIR/$name.keystore.jks" -deststorepass "$PASSWORD" -destkeypass "$PASSWORD" >/dev/null 2>&1
  keytool -importcert -noprompt -keystore "$CERT_DIR/$name.truststore.jks" -storepass "$PASSWORD" -alias ca \
    -file "$CERT_DIR/ca.crt" >/dev/null 2>&1
}

generate_client() {
  local name="$1"
  cat > "$CERT_DIR/$name.ext" <<EOT
extendedKeyUsage=clientAuth
EOT
  openssl req -new -newkey rsa:2048 -nodes -keyout "$CERT_DIR/$name.key" -out "$CERT_DIR/$name.csr" -subj "/CN=$name" >/dev/null 2>&1
  openssl x509 -req -in "$CERT_DIR/$name.csr" -CA "$CERT_DIR/ca.crt" -CAkey "$CERT_DIR/ca.key" -CAcreateserial \
    -out "$CERT_DIR/$name.crt" -days 3650 -sha256 -extfile "$CERT_DIR/$name.ext" >/dev/null 2>&1
  openssl pkcs12 -export -in "$CERT_DIR/$name.crt" -inkey "$CERT_DIR/$name.key" -certfile "$CERT_DIR/ca.crt" \
    -name "$name" -out "$CERT_DIR/$name.p12" -passout pass:"$PASSWORD" >/dev/null 2>&1
  keytool -importkeystore -noprompt -srckeystore "$CERT_DIR/$name.p12" -srcstoretype PKCS12 -srcstorepass "$PASSWORD" \
    -destkeystore "$CERT_DIR/$name.keystore.jks" -deststorepass "$PASSWORD" -destkeypass "$PASSWORD" >/dev/null 2>&1
}

generate_broker kafka-0 19092
generate_broker kafka-1 29092
generate_broker kafka-2 39092
for c in admin shop streams client connect analytics; do generate_client "$c"; done

for t in admin shop streams client connect analytics; do
  keytool -importcert -noprompt -keystore "$CERT_DIR/$t.truststore.jks" -storepass "$PASSWORD" -alias ca \
    -file "$CERT_DIR/ca.crt" >/dev/null 2>&1
 done

printf '%s' "$PASSWORD" > "$CERT_DIR/keystore_creds"
printf '%s' "$PASSWORD" > "$CERT_DIR/key_creds"
printf '%s' "$PASSWORD" > "$CERT_DIR/truststore_creds"
rm -f "$CERT_DIR"/*.csr "$CERT_DIR"/*.ext "$CERT_DIR"/*.p12 "$CERT_DIR/ca.srl"

echo "Certificates generated in $CERT_DIR"
