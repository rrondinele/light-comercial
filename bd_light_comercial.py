import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os
import io
import datetime
from urllib.parse import quote_plus

# --- 1. Configuração e Carregamento de Variáveis de Ambiente ---
load_dotenv()  # Carrega variáveis do arquivo .env

DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")

DB_PASS_ENCODED = quote_plus(DB_PASS)

SCHEMA_NAME = "light"
TABLE_NAME = '"4600010296_servicos"'

# Criar string de conexão SQLAlchemy
DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASS_ENCODED}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
print(f"URL de conexão: postgresql://{DB_USER}:{'*' * len(DB_PASS)}@{DB_HOST}:{DB_PORT}/{DB_NAME}")


@st.cache_resource(ttl=600)
def get_engine():
    """Cria engine SQLAlchemy para conexão com o banco"""
    print(f"CACHE MISS: Criando nova engine para o banco {DB_NAME}...")
    try:
        engine = create_engine(
            DATABASE_URL, 
            pool_pre_ping=True,
            echo=False  # Desativa logs para melhor performance
        )
        print("✅ Engine SQLAlchemy criada com sucesso!")
        return engine
    except Exception as e:
        print(f"❌ Erro ao criar engine: {e}")
        st.error(f"Erro ao conectar ao banco de dados: {e}")
        return None

@st.cache_data(ttl=300)
def fetch_data(query):
    """
    Busca dados do banco usando SQLAlchemy
    """
    print(f"CACHE MISS: Executando query: {query[:50]}...")
    
    engine = get_engine()
    if engine is None:
        return pd.DataFrame()
    
    try:
        with engine.connect() as conn:
            df = pd.read_sql(text(query), conn)
        return df
    except Exception as e:
        print(f"Erro ao buscar dados: {e}")
        st.error(f"Erro ao executar a query: {e}")
        return pd.DataFrame()

# --- Função específica para dados de início de turno ---
@st.cache_data(ttl=300)
def fetch_inicio_turno_data(data_inicio=None, data_fim=None, regional=None):
    """
    Busca dados de início de turno com filtros
    """
    query = f"""
    SELECT 
        s.tipo_atividade_1                                                                                      AS tipo_atividade,
        s.data_servico,
        TO_CHAR(s.inicio_servico, 'HH24:MI:SS')                                                                 AS inicio_servico,
        TO_CHAR(s.fim_servico, 'HH24:MI:SS')                                                                    AS fim_servico,        
        s.duracao,
        s.id_recurso,
        s.recurso,
        s.label_veiculo,
        split_part(s.idmatriculalider,'.',1)                                                                    AS idmatriculalider,
        split_part(s.idmatriculaauxiliares,'.',1)                                                               AS idmatriculaauxiliares,
        split_part(s.idmatriculaguarda,'.',1)                                                                   AS idmatriculaguarda,
        CASE 
            WHEN s.recurso LIKE '%BP%' THEN 'Barra do Piraí'
            WHEN s.recurso LIKE '%VR%' THEN 'Volta Redonda' 
            WHEN s.recurso LIKE '%TR%' THEN 'Três Rios'
            ELSE 'Outra'
        END                                                                                                     AS regional,
        CASE
            WHEN s.idmatriculalider IS NOT NULL AND s.idmatriculaauxiliares IS NULL THEN 'incompleta'
            ELSE 'completa'
        END                                                                                                     AS composicao
        
    FROM {SCHEMA_NAME}.{TABLE_NAME} s
    WHERE 1=1 
    AND s.tipo_atividade_1 = 'Início de turno'
    """
    
    # Aplica filtros
    conditions = []
    if data_inicio:
        conditions.append(f"s.data_servico >= '{data_inicio}'")
    if data_fim:
        conditions.append(f"s.data_servico <= '{data_fim}'")
    if regional and regional != "Todas":
        conditions.append(f"s.recurso LIKE '%{regional[:2]}%'")
    
    if conditions:
        query += " AND " + " AND ".join(conditions)
    
    query += " ORDER BY s.data_servico, s.inicio_servico"
    
    return fetch_data(query)

# --- Função para dados de drill down ---
@st.cache_data(ttl=300)
def fetch_drilldown_data(data_inicio=None, data_fim=None, regional=None):
    """
    Busca dados para análise de drill down (dia/mês/ano)
    """
    query = f"""
    SELECT 
        s.data_servico,
        EXTRACT(YEAR FROM s.data_servico) as ano,
        EXTRACT(MONTH FROM s.data_servico) as mes,
        EXTRACT(DAY FROM s.data_servico) as dia,
        s.recurso,
        CASE 
            WHEN s.recurso LIKE '%BP%' THEN 'Barra do Piraí'
            WHEN s.recurso LIKE '%VR%' THEN 'Volta Redonda' 
            WHEN s.recurso LIKE '%TR%' THEN 'Três Rios'
            ELSE 'Outra'
        END AS regional,
        CASE
            WHEN s.idmatriculalider IS NOT NULL AND s.idmatriculaauxiliares IS NULL THEN 'incompleta'
            ELSE 'completa'
        END AS composicao
    FROM {SCHEMA_NAME}.{TABLE_NAME} s
    WHERE 1=1 
    AND s.tipo_atividade_1 = 'Início de turno'
    """
    
    # Aplica filtros
    conditions = []
    if data_inicio:
        conditions.append(f"s.data_servico >= '{data_inicio}'")
    if data_fim:
        conditions.append(f"s.data_servico <= '{data_fim}'")
    if regional and regional != "Todas":
        conditions.append(f"s.recurso LIKE '%{regional[:2]}%'")
    
    if conditions:
        query += " AND " + " AND ".join(conditions)
    
    return fetch_data(query)

@st.cache_data(ttl=300)
def fetch_ofs_equipamentos(data_inicio=None, data_fim=None):
    """
    Busca dados da visão de equipamentos/notas (ofs_notas_equipamentos + serviços + lote_material)
    """
    query = f"""
    SELECT
        s.data_servico                                                              AS "Data",
        one.numero_nota                                                             AS "Nota",
        trim(substring(s.tipo_atividade_1 FROM ' - (.+)$'))                         AS "Texto Breve",
        one.secao_nome                                                              AS "Ação",
        CASE 
            WHEN s.status_atividade = 'concluído' THEN 'EXEC' 
            ELSE s.status_atividade 
        END                                                                         AS "Status Usuário",
        s.tipo_nota_servico                                                         AS "Tipo de Nota",
        trim(trailing '.0' from s.numero_instalacao)                                AS "Instalação",
        ''                                                                          AS "Zona",
        CASE 
            WHEN one.material IS NULL THEN l."lote" 
            ELSE ltrim(one.material, '0') 
        END                                                                         AS "Lote",
        --CASE 
        --    WHEN l.descricao IS NOT NULL THEN l.descricao
        --    WHEN one.descricao IS NOT NULL THEN one.descricao
        --   ELSE one.tipo_equipamento
        --END                                                                         as "Descricao",        
        COALESCE(l.descricao, one.descricao, one.tipo_equipamento) 					as "Descricao",
        TRIM(BOTH ' u' FROM one.quantidade)                                         AS "Quantidade",
        ltrim(one.numero_serie, '0')                                                AS "Serial",
        one.projeto                                                                 AS "Projeto",
        CASE
            WHEN 'L' || split_part(s.area_trabalho, ' - ', 2) IN (
                'L700','L705','L715','L716','L717','L722','L723','L731','L742','L745',
                'L747','L749','L754','L762','L763','L770','L830','L840'
            ) THEN 'Barra do Piraí'
            WHEN 'L' || split_part(s.area_trabalho, ' - ', 2) IN (
                'L646','L707','L710','L711','L713','L720','L721','L740','L741','L753',
                'L758','L760','L761','L786','L788','L793','L810','L825','L835','L850'
            ) THEN 'Três Rios'
            WHEN 'L' || split_part(s.area_trabalho, ' - ', 2) IN (
                'L735','L750','L752','L772','L776','L777','L778','L779','L782','L598'
            ) THEN 'Volta Redonda'
            ELSE '' 
        END                                                                         AS "Base Operacional"
    FROM {SCHEMA_NAME}.ofs_notas_equipamentos one
    LEFT JOIN {SCHEMA_NAME}.{TABLE_NAME} s 
        ON one.numero_nota = ltrim(s.ordem_servico, '0')
    LEFT JOIN {SCHEMA_NAME}.lote_material l 
        ON CASE
            WHEN one.tipo_equipamento = 'Lacre' 
                 AND one.dados_json->>'Tipo de Lacre' = 'SELO' 
                 AND s.tipo_nota_servico IN ('BB','BD')      THEN '391087'
            WHEN one.tipo_equipamento = 'Lacre' 
                 AND one.dados_json->>'Tipo de Lacre' = 'SELO' 
                 AND s.tipo_nota_servico NOT IN ('BB','BD') THEN '399127'
            WHEN one.tipo_equipamento = 'Lacre' 
                 AND one.dados_json->>'Tipo de Lacre' = 'TRAVA' THEN '399108'
            ELSE ltrim(one.material, '0') 
        END = l."lote"
    WHERE 1=1
    """

    # Filtro por período (reutilizando data_inicio / data_fim do sidebar)
    conditions = []
    if data_inicio:
        conditions.append(f"s.data_servico >= '{data_inicio}'")
    if data_fim:
        conditions.append(f"s.data_servico <= '{data_fim}'")

    if conditions:
        query += " AND " + " AND ".join(conditions)

    # Ordenação padrão
    query += ' ORDER BY s.data_servico, one.numero_nota'

    return fetch_data(query)


@st.cache_data(ttl=300)
def fetch_ofs_apr(data_inicio=None, data_fim=None):
    """
    Busca dados das Notas APR (ofs_apr + serviços)
    """
    query = f"""
    SELECT 
        s.data_servico                                     AS "Data",
        s.recurso                                          AS "Equipe",
        oa.numero_nota                                     AS "Nota",
        oa.card_numero                                     AS "Nº Pergunta",
        oa.pergunta_texto                                  AS "Pergunta",
        oa.item_numero                                     AS "Nº Item",
        oa.item_texto                                      AS "Item",
        oa.resposta                                        AS "Resposta"
    FROM {SCHEMA_NAME}.ofs_apr oa
    LEFT JOIN {SCHEMA_NAME}.{TABLE_NAME} s 
        ON oa.numero_nota = ltrim(s.ordem_servico, '0')
    WHERE 1=1
    """
    conditions = []
    if data_inicio:
        conditions.append(f"s.data_servico >= '{data_inicio}'")
    if data_fim:
        conditions.append(f"s.data_servico <= '{data_fim}'")

    if conditions:
        query += " AND " + " AND ".join(conditions)

    query += " ORDER BY s.data_servico, oa.numero_nota, oa.card_numero, oa.item_numero"

    return fetch_data(query)


# --- 3. Interface do Streamlit ---

# Configuração da página
st.set_page_config(layout="wide", page_title="Dashboard de Produtividade", page_icon="📊")

# Título
st.title("LIGHT Comercial - Dashboard de Indicadores ")
#st.markdown("Análise de serviços da tabela `light.\"4600010296_servicos\"`")

# --- Menu lateral com filtros ---
st.sidebar.title("🔧 Filtros e Navegação")

# Navegação por abas
aba_selecionada = st.sidebar.radio(
    "Navegação:",
    ["📊 Dashboard Geral", "🔄 Início de Turno", "🗺️ Mapa de Atividades", "🧰 Notas Equipamentos", "📝 Notas APR"]
)

# Filtros comuns no sidebar
st.sidebar.markdown("---")
st.sidebar.subheader("Filtros de Período")

# Data padrão: últimos 7 dias
data_hoje = datetime.date.today()
data_7_dias_atras = data_hoje - datetime.timedelta(days=7)

data_inicio = st.sidebar.date_input(
    "Data inicial:",
    value=data_7_dias_atras,
    max_value=data_hoje
)

data_fim = st.sidebar.date_input(
    "Data final:",
    value=data_hoje,
    max_value=data_hoje
)

# Filtro de regional
regional_opcoes = ["Todas", "Barra do Piraí", "Volta Redonda", "Três Rios"]
regional_selecionada = st.sidebar.selectbox(
    "Regional:",
    options=regional_opcoes
)

# Botão de atualização no sidebar
st.sidebar.markdown("---")
if st.sidebar.button("🔄 Atualizar Dados"):
    st.cache_data.clear()
    st.rerun()

# --- CONTEÚDO DAS ABAS ---

if aba_selecionada == "📊 Dashboard Geral":
    # Query 1: Contagem total por Status
    query_status = f"""
        SELECT 
            status_atividade, 
            COUNT(id_atividade) as total
        FROM {SCHEMA_NAME}.{TABLE_NAME}
        WHERE data_servico BETWEEN '{data_inicio}' AND '{data_fim}'
        GROUP BY status_atividade
        ORDER BY total DESC;
    """
    df_status = fetch_data(query_status)

    # Query 2: Contagem total por Equipe (Recurso)
    query_equipes = f"""
        SELECT 
            recurso,
            status_atividade,
            COUNT(id_atividade) as total
        FROM {SCHEMA_NAME}.{TABLE_NAME}
        WHERE recurso IS NOT NULL
        AND data_servico BETWEEN '{data_inicio}' AND '{data_fim}'
        GROUP BY recurso, status_atividade;
    """
    df_equipes = fetch_data(query_equipes)

    # Layout do Dashboard Geral
    st.header("📊 Visão Geral dos Status")
    
    if not df_status.empty:
        kpi_cols = st.columns(len(df_status))
        for i, row in df_status.iterrows():
            with kpi_cols[i]:
                st.metric(label=row['status_atividade'], value=row['total'])
    else:
        st.warning("Não foi possível carregar os KPIs de status.")

    st.divider()

    # Gráfico de Barras e Filtro por Equipe
    st.header("📈 Produtividade por Equipe")
    if not df_equipes.empty:
        equipes_lista = ["Todas"] + sorted(df_equipes['recurso'].unique())
        equipe_selecionada = st.selectbox("Selecione uma Equipe (Recurso):", equipes_lista)
        
        if equipe_selecionada == "Todas":
            df_equipes_filtrado = df_equipes.groupby('status_atividade')['total'].sum().reset_index()
        else:
            df_equipes_filtrado = df_equipes[df_equipes['recurso'] == equipe_selecionada]
            
        if not df_equipes_filtrado.empty:
            st.bar_chart(df_equipes_filtrado, x='status_atividade', y='total')
        else:
            st.info(f"Sem dados de status para a equipe '{equipe_selecionada}'.")
    else:
        st.warning("Não foi possível carregar os dados das equipes.")

elif aba_selecionada == "🔄 Início de Turno":
    st.header("🔄 Análise de Início de Turno")
    
    # Busca dados com filtros
    df_turno = fetch_inicio_turno_data(
        data_inicio=data_inicio, 
        data_fim=data_fim, 
        regional=regional_selecionada
    )
    
    if not df_turno.empty:
        # --- KPIs e Métricas ---
        st.subheader("📈 Métricas Principais")
        
        col1, col2, col3, col4, col5 = st.columns(5)
        
        with col1:
            total_recursos = df_turno['recurso'].nunique()
            st.metric("Total de Recursos", total_recursos)
        
        with col2:
            composicao_completa = (df_turno['composicao'] == 'completa').sum()
            st.metric("Composições Completas", composicao_completa)
        
        with col3:
            # Média de hora de início em formato HH:MM
            df_turno['hora_inicio'] = pd.to_datetime(df_turno['inicio_servico']).dt.time
            df_turno['minutos_inicio'] = pd.to_datetime(df_turno['inicio_servico']).dt.hour * 60 + pd.to_datetime(df_turno['inicio_servico']).dt.minute
            media_minutos_inicio = df_turno['minutos_inicio'].mean()
            horas = int(media_minutos_inicio // 60)
            minutos = int(media_minutos_inicio % 60)
            st.metric("Hora Média Início", f"{horas:02d}:{minutos:02d}")
        
        with col4:
            # Média de hora de fim em formato HH:MM
            df_turno['hora_fim'] = pd.to_datetime(df_turno['fim_servico']).dt.time
            df_turno['minutos_fim'] = pd.to_datetime(df_turno['fim_servico']).dt.hour * 60 + pd.to_datetime(df_turno['fim_servico']).dt.minute
            media_minutos_fim = df_turno['minutos_fim'].mean()
            horas = int(media_minutos_fim // 60)
            minutos = int(media_minutos_fim % 60)
            st.metric("Hora Média Fim", f"{horas:02d}:{minutos:02d}")
        
        with col5:
            # Média de recursos por dia no período
            recursos_por_dia = df_turno.groupby('data_servico')['recurso'].nunique()
            media_recursos_dia = recursos_por_dia.mean()
            st.metric("Média Recursos/Dia", f"{media_recursos_dia:.1f}")
        
        st.divider()
        
        # --- Análises Detalhadas ---
        st.subheader("📊 Análises por Data e Regional")
        
        # Quantidade de recursos por data
        recursos_por_data = df_turno.groupby('data_servico').agg({
            'recurso': 'nunique',
            'minutos_inicio': 'mean',
            'minutos_fim': 'mean'
        }).reset_index()
        
        # Converter minutos para formato HH:MM
        recursos_por_data['Hora Média Início'] = recursos_por_data['minutos_inicio'].apply(
            lambda x: f"{int(x//60):02d}:{int(x%60):02d}" if not pd.isna(x) else "N/A"
        )
        recursos_por_data['Hora Média Fim'] = recursos_por_data['minutos_fim'].apply(
            lambda x: f"{int(x//60):02d}:{int(x%60):02d}" if not pd.isna(x) else "N/A"
        )
        
        recursos_por_data = recursos_por_data[['data_servico', 'recurso', 'Hora Média Início', 'Hora Média Fim']]
        recursos_por_data.columns = ['Data', 'Qtd Recursos', 'Hora Média Início', 'Hora Média Fim']
        
        col_analise1, col_analise2 = st.columns(2)
        
        with col_analise1:
            st.markdown("**Recursos por Data**")
            st.dataframe(recursos_por_data, use_container_width=True, hide_index=True)
        
        with col_analise2:
            st.markdown("**Composição por Regional**")
            composicao_regional = df_turno.groupby(['regional', 'composicao']).size().unstack(fill_value=0)
            # Adicionar coluna de total
            composicao_regional['Total'] = composicao_regional.sum(axis=1)
            st.dataframe(composicao_regional, use_container_width=True)
        
        st.divider()
        
        # --- Gráfico de Drill Down ---
        st.subheader("📊 Evolução de Equipes por Período")

        # Buscar dados para drill down
        df_drilldown = fetch_drilldown_data(
            data_inicio=data_inicio, 
            data_fim=data_fim, 
            regional=regional_selecionada
        )

        if not df_drilldown.empty:
            # Selecionar nível de agrupamento
            nivel_agrupamento = st.selectbox(
                "Agrupar por:",
                ["Dia", "Mês", "Ano"],
                key="drilldown_level"
            )
            
            if nivel_agrupamento == "Dia":
                # Agrupar por data_servico - garantir que só temos colunas numéricas
                df_agrupado = df_drilldown.groupby(['data_servico', 'composicao']).size().unstack(fill_value=0).reset_index()
                # Somar apenas colunas numéricas (completa, incompleta)
                colunas_numericas = [col for col in df_agrupado.columns if col not in ['data_servico', 'mes', 'ano', 'dia']]
                df_agrupado['Total'] = df_agrupado[colunas_numericas].sum(axis=1)
                x_axis = 'data_servico'
                
            elif nivel_agrupamento == "Mês":
                # Agrupar por mês - converter para string para evitar problemas
                df_drilldown['mes_str'] = df_drilldown['mes'].astype(int).astype(str) + '/' + df_drilldown['ano'].astype(int).astype(str)
                df_agrupado = df_drilldown.groupby(['mes_str', 'composicao']).size().unstack(fill_value=0).reset_index()
                colunas_numericas = [col for col in df_agrupado.columns if col not in ['mes_str', 'data_servico', 'mes', 'ano', 'dia']]
                df_agrupado['Total'] = df_agrupado[colunas_numericas].sum(axis=1)
                x_axis = 'mes_str'
                
            else:  # Ano
                # Agrupar por ano
                df_drilldown['ano_str'] = df_drilldown['ano'].astype(int).astype(str)
                df_agrupado = df_drilldown.groupby(['ano_str', 'composicao']).size().unstack(fill_value=0).reset_index()
                colunas_numericas = [col for col in df_agrupado.columns if col not in ['ano_str', 'data_servico', 'mes', 'ano', 'dia']]
                df_agrupado['Total'] = df_agrupado[colunas_numericas].sum(axis=1)
                x_axis = 'ano_str'
            
            # Criar gráfico de barras empilhadas
            colunas_grafico = [col for col in ['completa', 'incompleta'] if col in df_agrupado.columns]
            
            if len(colunas_grafico) >= 1:
                chart_data = df_agrupado.set_index(x_axis)[colunas_grafico]
                st.bar_chart(chart_data, use_container_width=True)
                
                # Mostrar tabela de dados também
                with st.expander("📋 Ver dados detalhados"):
                    st.dataframe(df_agrupado, use_container_width=True, hide_index=True)
            else:
                st.info("Dados insuficientes para gerar o gráfico de drill down.")
        
        st.divider()
        
        # --- Tabela Detalhada ---
        st.subheader("📋 Dados Detalhados de Início de Turno")
        
        # Filtros na tabela
        col_filtro1, col_filtro2 = st.columns(2)
        
        with col_filtro1:
            composicao_filtro = st.selectbox(
                "Filtrar por Composição:",
                ["Todas", "completa", "incompleta"]
            )
        
        with col_filtro2:
            recursos_disponiveis = ["Todos"] + sorted(df_turno['recurso'].unique())
            recurso_filtro = st.selectbox("Filtrar por Recurso:", recursos_disponiveis)
        
        # Aplica filtros na tabela
        df_turno_filtrado = df_turno.copy()
        if composicao_filtro != "Todas":
            df_turno_filtrado = df_turno_filtrado[df_turno_filtrado['composicao'] == composicao_filtro]
        if recurso_filtro != "Todos":
            df_turno_filtrado = df_turno_filtrado[df_turno_filtrado['recurso'] == recurso_filtro]
        
        # Mostra tabela
        colunas_ocultas = ["hora_inicio", "minutos_inicio", "hora_fim", "minutos_fim"]

        df_exibir = df_turno_filtrado.drop(columns=[c for c in colunas_ocultas if c in df_turno_filtrado.columns])

        st.dataframe(
            df_exibir,
            use_container_width=True,
            hide_index=True
        )
        # st.dataframe(
        #     df_turno_filtrado,
        #     use_container_width=True,
        #     hide_index=True
        # )
        
        # Botão de download
        csv = df_turno_filtrado.to_csv(index=False)
        st.download_button(
            label="📥 Download CSV",
            data=csv,
            file_name=f"inicio_turno_{data_inicio}_a_{data_fim}.csv",
            mime="text/csv"
        )
        
    else:
        st.warning("⚠️ Nenhum dado encontrado para os filtros selecionados.")

elif aba_selecionada == "🗺️ Mapa de Atividades":
    st.header("🗺️ Mapa de Atividades")
    
    # Query para o mapa (mantendo sua query original)
    query_mapa = f"""
        SELECT 
            id_atividade,
            recurso,
            status_atividade,
            coordenada_x,
            coordenada_y
        FROM {SCHEMA_NAME}.{TABLE_NAME}
        WHERE 
            coordenada_x IS NOT NULL 
            AND coordenada_y IS NOT NULL
            AND data_servico BETWEEN '{data_inicio}' AND '{data_fim}'
            AND status_atividade = 'pendente'
    """
    df_mapa_bruto = fetch_data(query_mapa)
    
    if not df_mapa_bruto.empty:
        df_mapa = df_mapa_bruto.rename(columns={'coordenada_y': 'lat', 'coordenada_x': 'lon'})
        
        equipes_mapa_lista = ["Todas"] + sorted(df_mapa['recurso'].unique())
        equipe_mapa_selecionada = st.selectbox("Filtrar mapa por Equipe:", equipes_mapa_lista)
        
        if equipe_mapa_selecionada == "Todas":
            st.map(df_mapa[['lat', 'lon']])
        else:
            df_mapa_filtrado = df_mapa[df_mapa['recurso'] == equipe_mapa_selecionada]
            if not df_mapa_filtrado.empty:
                st.map(df_mapa_filtrado[['lat', 'lon']])
            else:
                st.info(f"Sem atividades no mapa para a equipe '{equipe_mapa_selecionada}'.")
    else:
        st.warning("Não foi possível carregar dados de geolocalização para o mapa.")

elif aba_selecionada == "🧰 Notas Equipamentos":
    st.header("🧰 Visão de Notas Equipamentos")

    def parse_multi_filter(text: str):
        """
        Converte um texto em lista de termos, aceitando separadores
        como vírgula, ponto e vírgula e espaços.
        Ex: "123, 456;789 0001" -> ["123", "456", "789", "0001"]
        """
        if not text:
            return []
        # Normaliza separadores para espaço
        for sep in [",", ";"]:
            text = text.replace(sep, " ")
        # Quebra em partes, removendo vazios
        parts = [p.strip() for p in text.split() if p.strip()]
        return parts

    # Busca dados com filtros de período globais (sidebar)
    df_equip = fetch_ofs_equipamentos(
        data_inicio=data_inicio,
        data_fim=data_fim
    )

    if df_equip.empty:
        st.warning("⚠️ Nenhum dado encontrado para os filtros selecionados.")
    else:
        # ----------------------------
        # FILTROS ESPECÍFICOS DA ABA
        # ----------------------------
        with st.expander("🎛️ Filtros adicionais", expanded=True):
            # Linha 1: datas + nota
            col1, col2, col3 = st.columns(3)
            with col1:
                data_ini_local = st.date_input(
                    "Data inicial",
                    value=data_inicio,
                    key="equip_data_ini"
                )
            with col2:
                data_fim_local = st.date_input(
                    "Data final",
                    value=data_fim,
                    key="equip_data_fim"
                )
            with col3:
                filtro_nota = st.text_input(
                    "Nota (+ Lista)",
                    key="filtro_nota"
                )

            # Linha 2: lote + serial
            col4, col5 = st.columns(2)
            with col4:
                filtro_lote = st.text_input(
                    "Lote (+ Lista)",
                    key="filtro_lote"
                )
            with col5:
                filtro_serial = st.text_input(
                    "Serial (+ Lista)",
                    key="filtro_serial"
                )

            # Linha 3: Base Operacional + Ação (multiselect)
            col6, col7 = st.columns(2)

            # Garantir que as colunas existem antes de criar as opções
            bases_sel = []
            acoes_sel = []

            with col6:
                if "Base Operacional" in df_equip.columns:
                    opcoes_bases = sorted(
                        [b for b in df_equip["Base Operacional"].dropna().unique()]
                    )
                    bases_sel = st.multiselect(
                        "Base Operacional (multiseleção)",
                        options=opcoes_bases,
                        default=[]
                    )

            with col7:
                # Coluna pode estar como "Ação" ou "Acao"
                col_acao = None
                if "Ação" in df_equip.columns:
                    col_acao = "Ação"
                elif "Acao" in df_equip.columns:
                    col_acao = "Acao"

                if col_acao:
                    opcoes_acoes = sorted(
                        [a for a in df_equip[col_acao].dropna().unique()]
                    )
                    acoes_sel = st.multiselect(
                        "Ação (multiseleção)",
                        options=opcoes_acoes,
                        default=[]
                    )

        # ----------------------------
        # APLICAÇÃO DOS FILTROS
        # ----------------------------
        df_filtrado = df_equip.copy()

        # 1) Período (refino em cima da query)
        if "Data" in df_filtrado.columns:
            df_filtrado["Data"] = pd.to_datetime(df_filtrado["Data"]).dt.date
            if data_ini_local:
                df_filtrado = df_filtrado[df_filtrado["Data"] >= data_ini_local]
            if data_fim_local:
                df_filtrado = df_filtrado[df_filtrado["Data"] <= data_fim_local]

        # 2) Filtro por Nota (lista flexível)
        lista_notas = parse_multi_filter(filtro_nota)
        if lista_notas:
            df_filtrado = df_filtrado[
                df_filtrado["Nota"].astype(str).isin(lista_notas)
            ]

        # 3) Filtro por Lote (lista flexível)
        lista_lotes = parse_multi_filter(filtro_lote)
        if lista_lotes and "Lote" in df_filtrado.columns:
            df_filtrado = df_filtrado[
                df_filtrado["Lote"].astype(str).isin(lista_lotes)
            ]

        # 4) Filtro por Serial (lista flexível)
        lista_seriais = parse_multi_filter(filtro_serial)
        if lista_seriais and "Serial" in df_filtrado.columns:
            df_filtrado = df_filtrado[
                df_filtrado["Serial"].astype(str).isin(lista_seriais)
            ]

        # 5) Filtro por Base Operacional (multiselect)
        if bases_sel and "Base Operacional" in df_filtrado.columns:
            df_filtrado = df_filtrado[
                df_filtrado["Base Operacional"].isin(bases_sel)
            ]

        # 6) Filtro por Ação (multiselect)
        if "Ação" in df_filtrado.columns:
            col_acao = "Ação"
        elif "Acao" in df_filtrado.columns:
            col_acao = "Acao"
        else:
            col_acao = None

        if col_acao and acoes_sel:
            df_filtrado = df_filtrado[
                df_filtrado[col_acao].isin(acoes_sel)
            ]

        # ----------------------------
        # KPIs SIMPLES
        # ----------------------------
        col1, col2 = st.columns(2)
        with col1:
            st.metric("Total de Registros", len(df_filtrado))
        with col2:
            st.metric("Notas", df_filtrado["Nota"].nunique())

        st.divider()

        # ----------------------------
        # TABELA + DOWNLOAD
        # ----------------------------

        def to_excel(df):
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine="openpyxl") as writer:
                df.to_excel(writer, index=False, sheet_name="Dados")
            processed_data = output.getvalue()
            return processed_data


        st.subheader("📋 Detalhamento de Notas Equipamentos")
        st.dataframe(
            df_filtrado,
            use_container_width=True,
            hide_index=True
        )

        excel_file = to_excel(df_filtrado)
        st.download_button(
            label="📥 Download Excel (.xlsx)",
            data=excel_file,
            file_name=f"ofs_equipamentos_{data_ini_local}_a_{data_fim_local}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

elif aba_selecionada == "📝 Notas APR":
    st.header("📝 Notas APR")

    # Busca dados de APR usando o período global do sidebar
    df_apr = fetch_ofs_apr(
        data_inicio=data_inicio,
        data_fim=data_fim
    )

    if df_apr.empty:
        st.warning("⚠️ Nenhum registro de APR encontrado para o período selecionado.")
    else:
        # Filtros simples na própria aba (opcionais, mas úteis)
        with st.expander("🎛️ Filtros adicionais", expanded=False):
            col1, col2 = st.columns(2)
            with col1:
                equipes = ["Todas"] + sorted(df_apr["Equipe"].dropna().unique().tolist())
                equipe_sel = st.selectbox("Filtrar por Equipe:", equipes)
            with col2:
                nota_sel = st.text_input("Filtrar por Nota específica (ex: 1625861939)")

        df_filtrado = df_apr.copy()

        if equipe_sel != "Todas":
            df_filtrado = df_filtrado[df_filtrado["Equipe"] == equipe_sel]

        if nota_sel:
            df_filtrado = df_filtrado[df_filtrado["Nota"].astype(str) == nota_sel.strip()]

        st.subheader("📋 Detalhamento das Notas APR")
        st.dataframe(
            df_filtrado,
            use_container_width=True,
            hide_index=True
        )

        # Função auxiliar para exportar Excel
        def apr_to_excel(df: pd.DataFrame) -> bytes:
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine="openpyxl") as writer:
                df.to_excel(writer, index=False, sheet_name="Notas APR")
            return output.getvalue()

        excel_bytes = apr_to_excel(df_filtrado)

        st.download_button(
            label="📥 Download Excel (.xlsx)",
            data=excel_bytes,
            file_name=f"ofs_apr_{data_inicio}_a_{data_fim}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
