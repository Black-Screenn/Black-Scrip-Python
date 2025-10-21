from geopy.geocoders import Nominatim

# Initialize the geolocator with a user agent
# Replace "my_geocoder_app" with a unique name for your application
geolocator = Nominatim(user_agent="my_geocoder_app")

# Define the address to geocode
address = "Rua João Antônio de Oliveira"

# Geocode the address
location = geolocator.geocode(address)

# Check if a location was found
if location:
    print(f"Address: {location.address}")
    print(f"Latitude: {location.latitude}")
    print(f"Longitude: {location.longitude}")
else:
    print(f"Could not find coordinates for: {address}")