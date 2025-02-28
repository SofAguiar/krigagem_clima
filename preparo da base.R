library(readr)
library(dplyr)
library(stringr)
library(tools)

# Define o caminho onde os arquivos CSV do INMET estão localizados
caminho <- "previsão dengue/2018/2018/"

# Lista os arquivos que contêm "INMET_SE_MG" e terminam com .csv (ignorando maiúsculas/minúsculas)
arquivos <- list.files(
  path = caminho, 
  pattern = "INMET_SE_MG.*\\.[Cc][Ss][Vv]$", 
  full.names = TRUE
)

# Lê os arquivos e extrai código e cidade do nome do arquivo
dados <- bind_rows(lapply(arquivos, function(arquivo) {
  nome_arquivo <- basename(arquivo)
  nome_sem_extensao <- file_path_sans_ext(nome_arquivo)
  nome_sem_prefixo <- str_remove(nome_sem_extensao, "^INMET_SE_MG_")
  partes <- str_split(nome_sem_prefixo, "_")[[1]]
  codigo <- partes[1]
  cidade <- partes[2]
  
  df <- read_csv2(arquivo, skip = 8, locale = locale(encoding = "latin1"))
  df <- df %>% mutate(codigo = codigo, cidade = cidade) %>% select(-...20)
  return(df)
}))

# Salva os dados consolidados
write.csv2(dados, "previsão dengue/mg_2019.csv")

###### Repita esse processo para cada ano de interesse

# Carrega os dados históricos previamente consolidados
mg16 <- read.csv2("previsão dengue/mg_2016.csv")
mg17 <- read.csv2("previsão dengue/mg_2017.csv")
mg18 <- read.csv2("previsão dengue/mg_2018.csv")

# Combina os anos 
mg_16a18 <- bind_rows(mg16, mg17, mg18) %>% 
  rename(Data = DATA..YYYY.MM.DD., Hora.UTC = HORA..UTC.)

# Substitui valores problemáticos por NA antes da agregação
mg_16a18 <- mg_16a18 %>% 
  mutate(across(where(is.integer), as.double)) %>%
  mutate(across(where(is.double), ~na_if(., -9999))) %>%
  mutate(across(where(is.double), ~na_if(., 9999))) %>%
  mutate(across(where(is.double), ~na_if(., -Inf))) %>% 
  mutate(across(where(is.double), ~na_if(., Inf)))



# Agregação dos dados por código de estação e data
mg_total_group <- mg_16a18 %>% 
  group_by(codigo, cidade, Data) %>% 
  summarise(
    preciptacao = sum(PRECIPITAÇÃO.TOTAL..HORÁRIO..mm., na.rm = TRUE),
    pressao_atmosferica_media = mean(PRESSAO.ATMOSFERICA.AO.NIVEL.DA.ESTACAO..HORARIA..mB., na.rm = TRUE),
    temperatura_max = max(TEMPERATURA.DO.AR...BULBO.SECO..HORARIA...C., na.rm = TRUE),
    temperatura_min = min(TEMPERATURA.DO.AR...BULBO.SECO..HORARIA...C., na.rm = TRUE),
    temperatura_media = mean(TEMPERATURA.DO.AR...BULBO.SECO..HORARIA...C., na.rm = TRUE),
    umidade_maxima = max(UMIDADE.RELATIVA.DO.AR..HORARIA...., na.rm = TRUE),
    umidade_minima = min(UMIDADE.RELATIVA.DO.AR..HORARIA...., na.rm = TRUE),
    umidade_media = mean(UMIDADE.RELATIVA.DO.AR..HORARIA...., na.rm = TRUE),
    .groups = "drop"
  )


# Carrega e filtra catálogo de estações meteorológicas
CatalogoEstaçõesAutomáticas <- read_delim("previsão dengue/CatalogoEstaçõesAutomáticas.csv", 
                                          delim = ";", escape_double = FALSE, 
                                          locale = locale(decimal_mark = ","), 
                                          trim_ws = TRUE)

Catalogo <- CatalogoEstaçõesAutomáticas %>% 
  filter(SG_ESTADO == "MG") %>% 
  select(CD_ESTACAO, VL_LATITUDE, VL_LONGITUDE) %>% 
  rename(codigo = CD_ESTACAO)

# Junta os dados agregados com o catálogo de estações
mg_total_final <- left_join(mg_total_group, Catalogo, by = "codigo") %>% 
  filter(!is.na(VL_LATITUDE)) %>%
  mutate(across(where(is.integer), as.double)) %>%  # Converte inteiros para double
  mutate(across(where(is.double), ~na_if(., -9999))) %>%
  mutate(across(where(is.double), ~na_if(., 9999))) %>%
  mutate(across(where(is.double), ~na_if(., -Inf))) %>%
  mutate(across(where(is.double), ~na_if(., Inf)))

# Conferindo o banco de dados criado
summary(mg_total_final)

head(mg_total_final)

# Salva os dados finais
write.csv(mg_total_final, "previsão dengue/estacoes_mg.csv")
