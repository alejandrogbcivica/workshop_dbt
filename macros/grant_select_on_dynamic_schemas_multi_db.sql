{% macro grant_select_on_dynamic_schemas_multi_db(operator_role) %}
  
  {# 1. Variables de Contexto y Bases de Datos #}
  {% set schema_prefix = target.schema | trim %}
  {% set is_ci = env_var('DBT_ENVIRONMENT', 'dev') == 'ci' %}

  {# Lista de Bases de Datos de CI donde buscar esquemas temporales #}
  {% set ci_databases = ['WORKSHOP_CI_GOLD', 'WORKSHOP_CI_SILVER'] %}

  {% if execute and is_ci %}
    
    {{ log("Iniciando concesión de permisos SELECT dinámicos para el prefijo: " ~ schema_prefix ~ " al rol: " ~ operator_role, info=True) }}

    {# 2. ITERAR SOBRE AMBAS BASES DE DATOS #}
    {% for db_name in ci_databases %}
      
      {{ log("--- Procesando Database: " ~ db_name ~ " ---", info=True) }}

      {# A. Conceder USAGE en la DATABASE (Obligatorio para acceder) #}
      {% set grant_db_usage_sql %}
        GRANT USAGE ON DATABASE {{ db_name }} TO ROLE {{ operator_role }};
      {% endset %}
      {% do run_query(grant_db_usage_sql) %}
      {{ log("✅ USAGE en la DATABASE " ~ db_name ~ " concedido.", info=True) }}

      {# B. Consultar dinámicamente los esquemas con el prefijo de la PR #}
      {% set find_schemas_sql %}
        SELECT schema_name 
        FROM {{ db_name }}.INFORMATION_SCHEMA.SCHEMATA
        WHERE schema_name ILIKE '{{ schema_prefix }}%' 
      {% endset %}

      {% set schemas_to_grant = run_query(find_schemas_sql) %}
      {% set schema_count = schemas_to_grant.rows | length %}
      
      {{ log("Encontrados " ~ schema_count ~ " esquemas temporales en " ~ db_name ~ ".", info=True) }}

      {# C. ITERAR y ejecutar los GRANTS para cada esquema y objetos #}
      {% for row in schemas_to_grant.rows %}
        
        {% set schema_name = row.values()[0] %}
        
        {% set grant_sql %}
          -- 1. Otorgar USAGE en el esquema
          GRANT USAGE ON SCHEMA {{ db_name }}.{{ schema_name }} TO ROLE {{ operator_role }};
          
          -- 2. Otorgar SELECT en todas las Tablas/Vistas existentes
          GRANT SELECT ON ALL TABLES IN SCHEMA {{ db_name }}.{{ schema_name }} TO ROLE {{ operator_role }};
          GRANT SELECT ON ALL VIEWS IN SCHEMA {{ db_name }}.{{ schema_name }} TO ROLE {{ operator_role }};
          
        {% endset %}
        
        {% do run_query(grant_sql) %}
        {{ log("✅ SELECT otorgado en " ~ db_name ~ "." ~ schema_name, info=True) }}
        
      {% endfor %}
    
    {% endfor %}

  {% elif execute %}
    
    {# ❌ Este es el log que se muestra si execute es verdadero pero is_ci es falso #}
    {{ log("Skipping: Esta operación de permisos está configurada para ejecutarse SOLAMENTE en el entorno de Integración Continua (CI).", info=True) }}

  {% endif %}

{% endmacro %}