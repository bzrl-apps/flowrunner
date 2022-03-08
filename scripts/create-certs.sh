
set -o nounset \
    -o errexit \
    -o verbose \
    -o xtrace

rm -rf secrets 2>&1
mkdir -p secrets

pushd secrets

# Generate CA key
openssl req -new -x509 -keyout kafka-ca.key -out kafka-ca.crt -days 9999 -subj '/CN=ca-broker' -passin pass:flowrunner -passout pass:flowrunner
# openssl req -new -x509 -keyout snakeoil-ca-2.key -out snakeoil-ca-2.crt -days 365 -subj '/CN=ca2.test.flowrunner.io/OU=TEST/O=CONFLUENT/L=PaloAlto/S=Ca/C=US' -passin pass:flowrunner -passout pass:flowrunner

# Consumer
openssl genrsa -des3 -passout "pass:flowrunner" -out consumer.client.key 1024
openssl req -passin "pass:flowrunner" -passout "pass:flowrunner" -key consumer.client.key -new -out consumer.client.req -subj '/CN=consumer'
openssl x509 -req -CA kafka-ca.crt -CAkey kafka-ca.key -in consumer.client.req -out consumer-ca-signed.pem -days 9999 -CAcreateserial -passin "pass:flowrunner"

# Producer
openssl genrsa -des3 -passout "pass:flowrunner" -out producer.client.key 1024
openssl req -passin "pass:flowrunner" -passout "pass:flowrunner" -key producer.client.key -new -out producer.client.req -subj '/CN=producer'
openssl x509 -req -CA kafka-ca.crt -CAkey kafka-ca.key -in producer.client.req -out producer-ca-signed.pem -days 9999 -CAcreateserial -passin "pass:flowrunner"


for i in broker
do
	echo $i
	# Create keystores
	keytool -genkey -noprompt \
				 -alias $i \
				 -dname "CN=$i" \
				 -keystore $i.keystore.jks \
				 -keyalg RSA \
				 -storepass flowrunner \
				 -keypass flowrunner

	# Create CSR, sign the key and import back into keystore
	keytool -keystore $i.keystore.jks -alias $i -certreq -file $i.csr -storepass flowrunner -keypass flowrunner

	openssl x509 -req -CA kafka-ca.crt -CAkey kafka-ca.key -in $i.csr -out $i-ca-signed.crt -days 9999 -CAcreateserial -passin pass:flowrunner

	keytool -keystore $i.keystore.jks -alias CARoot -import -file kafka-ca.crt -storepass flowrunner -keypass flowrunner

	keytool -keystore $i.keystore.jks -alias $i -import -file $i-ca-signed.crt -storepass flowrunner -keypass flowrunner

	# Create truststore and import the CA cert.
	keytool -keystore $i.truststore.jks -alias CARoot -import -file kafka-ca.crt -storepass flowrunner -keypass flowrunner

  echo "flowrunner" > ${i}_sslkey_creds
  echo "flowrunner" > ${i}_keystore_creds
  echo "flowrunner" > ${i}_truststore_creds
done

popd
