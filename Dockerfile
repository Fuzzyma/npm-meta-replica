# Use the official CouchDB image as the base
FROM couchdb:3.3

# Set environment variables for CouchDB
ENV COUCHDB_USER=admin \
    COUCHDB_PASSWORD=admin

# Expose CouchDB ports
EXPOSE 5984 5986 4369

# Start CouchDB
CMD ["couchdb"]
