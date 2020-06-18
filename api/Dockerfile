FROM continuumio/miniconda3:latest
LABEL maintainer="cuong.vo2@tiki.vn"

ARG workspace=/app

COPY api.yml .
RUN conda update -n base -c defaults conda \
    && conda config --add channels conda-forge \
    && conda env create -f api.yml \
    && rm -rf /opt/conda/pkgs/* && rm -rf /tmp/api.yml

WORKDIR $workspace

# Copy all files after to avoid rebuild the conda env each time
COPY ./api ./api

# Launch the API
ENTRYPOINT ["/opt/conda/envs/api/bin/python"]
CMD ["app.py"]