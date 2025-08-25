from google.cloud import secretmanager

client = secretmanager.SecretManagerServiceClient()
name = "projects/555005178535/secrets/email_app_password/versions/latest"
response = client.access_secret_version(request={"name": name})
secret_value = response.payload.data.decode("UTF-8")

print(secret_value)
