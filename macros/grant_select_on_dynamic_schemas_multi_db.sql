{% macro grant_temporal_schemas_ci(operator_role) %}
  
  {# 1. Context Variables and Dynamic DBs #}
  {% set schema_prefix = target.schema | trim %}
  {% set dbt_env = env_var('DBT_ENVIRONMENT', 'dev') %}
  {% set is_ci = dbt_env == 'ci' %}

  {% set security_admin_role = 'SECURITYADMIN' %}

  {# Dynamically define the list of CI Databases: WORKSHOP_<ENV>_GOLD/SILVER #}
  {% set ci_databases = ['WORKSHOP_' ~ dbt_env | upper ~ '_GOLD', 'WORKSHOP_' ~ dbt_env | upper ~ '_SILVER'] %}

  {% if execute and is_ci %}
    
    {{ log("Starting dynamic SELECT grants for prefix: " ~ schema_prefix ~ " to role: " ~ operator_role, info=True) }}


    {# Swicth to securityadmin for grants #}

    {% call statement('use_security_admin') %}
      USE ROLE {{ security_admin_role }};
    {% endcall %}
    {{ log("Switched role to " ~ security_admin_role ~ " for permissions granting.", info=True) }}

    {# 2. Iterate over all ci databases #}
    {% for db_name in ci_databases %}
      
      {{ log("--- Processing Database: " ~ db_name ~ " ---", info=True) }}

      {# A. Grant USAGE on the DATABASE (Required for top-level access) #}
      {% set grant_db_usage_sql %}
        GRANT USAGE ON DATABASE {{ db_name }} TO ROLE {{ operator_role }};
      {% endset %}
      {% do run_query(grant_db_usage_sql) %}
      
      {# B. Query for schemas matching the PR prefix (using ILIKE for case-insensitivity) #}
      {% set find_schemas_sql %}
        SELECT schema_name 
        FROM {{ db_name }}.INFORMATION_SCHEMA.SCHEMATA
        WHERE schema_name ILIKE '{{ schema_prefix }}%' 
      {% endset %}

      {% set schemas_to_grant = run_query(find_schemas_sql) %}
      {% set schema_count = schemas_to_grant.rows | length %}
      
      {{ log("Found " ~ schema_count ~ " temporary schemas in " ~ db_name ~ ".", info=True) }}

      {# C. Iterate and execute grants for each schema and its objects #}
      {% for row in schemas_to_grant.rows %}
        
        {% set schema_name = row.values()[0] %}
        
        {% set grant_sql %}
          -- 1. Grant USAGE on the schema
          GRANT USAGE ON SCHEMA {{ db_name }}.{{ schema_name }} TO ROLE {{ operator_role }};
          
          -- 2. Grant SELECT on all existing Tables/Views
          GRANT SELECT ON ALL TABLES IN SCHEMA {{ db_name }}.{{ schema_name }} TO ROLE {{ operator_role }};
          GRANT SELECT ON ALL VIEWS IN SCHEMA {{ db_name }}.{{ schema_name }} TO ROLE {{ operator_role }};
          GRANT SELECT ON DYNAMIC TABLES IN SCHEMA {{ db_name }}.{{ schema_name }} TO ROLE {{ operator_role }};
        {% endset %}
        
        {% do run_query(grant_sql) %}
        {{ log("Successfully granted SELECT permissions on schema: " ~ db_name ~ "." ~ schema_name, info=True) }}
        
      {% endfor %}
    
    {% endfor %}

  {% endif %}

{% endmacro %}