FROM jupyter/scipy-notebook:python-3.9

ARG BEAM_VERSION
ENV BEAM_VERSION=${PYTHON_VERSION:-2.53.0}

USER root

RUN apt-get update -y && apt-get install -y graphviz

USER ${NB_UID}

RUN pip install "apache-beam[test,interactive]==${BEAM_VERSION}"
# to fix the following error
#   AttributeError: module 'lib' has no attribute 'X509_V_FLAG_CB_ISSUER_CHECK'
RUN pip install pyopenssl --upgrade
RUN jupyter labextension install @jupyter-widgets/jupyterlab-manager
