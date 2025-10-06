FROM ghcr.io/osgeo/gdal:ubuntu-full-latest

COPY convert_all.sh /convert_all.sh
RUN chmod +x /convert_all.sh

# Ange arbetskatalog och körs som standard
WORKDIR /data
ENTRYPOINT ["/convert_all.sh"]
