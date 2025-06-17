FROM astrocrpublic.azurecr.io/runtime:3.0-4

# Set environment variables directly
ENV AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_ALL_ADMINS=False
ENV AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_PASSWORDS_PATH=/usr/local/airflow/simple_auth_manager_passwords.json

USER root
COPY simple_auth_manager_passwords.json /usr/local/airflow/simple_auth_manager_passwords.json
RUN chmod 644 /usr/local/airflow/simple_auth_manager_passwords.json && \
    chown astro:astro /usr/local/airflow/simple_auth_manager_passwords.json
USER astro