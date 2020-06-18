FROM continuumio/miniconda3:latest

LABEL maintainer="cuong.vo2@tiki.vn"

ARG workspace=/app

WORKDIR /tmp
COPY scheduler.yaml .

RUN conda update -n base -c defaults conda \
    && conda config --add channels conda-forge \
    && conda env create -f scheduler.yaml \
    && rm -rf /opt/conda/pkgs/* && rm -rf /tmp/scheduler.yaml

# SHELL ["/bin/bash", "--login", "-c"]
# RUN conda init bash && conda activate scheduler
# RUN echo "conda activate scheduler" >> ~/.bashrc

WORKDIR $workspace

# Copy all files after to avoid rebuild the conda env each time
COPY ./dycounter ./dycounter
COPY ./runner.py .

# Launch the API
ENTRYPOINT ["/opt/conda/envs/scheduler/bin/python"]
CMD ["runner.py"]