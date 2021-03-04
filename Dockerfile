FROM python:3.6

RUN apt-get update

COPY requirements.txt /
RUN pip3 install -r requirements.txt


# Copy in all of our code files.
COPY crawlers /crawlers

ARG xtract_db=xtract_db
ARG xtract_pass=xtract_pass
ARG aws_access=aws_access
ARG aws_secret=aws_secret

ENV XTRACT_DB=$xtract_db
ENV XTRACT_PASS=$xtract_pass
ENV AWS_ACCESS=$aws_access
ENV AWS_SECRET=$aws_secret

ARG globus_client=globus_client
ARG globus_secret=globus_secret

ENV globus_client=$globus_client
ENV globus_secret=$globus_secret

EXPOSE 5432
EXPOSE 80

CMD ["python", "/crawlers/run.py"]
