# from langchain.embeddings.openai import OpenAIEmbeddings
# from langchain.embeddings import AzureOpenAIEmbeddings

from .maum_embedding import MaumEmebedding

def create_embedding(cfg, model):
  return MaumEmebedding(cfg, model)
  # input args for later use
  # elif embed_type == "azure":
  #   return AzureOpenAIEmbeddings()
# def create_embedding(cfg, model, embed_type):
#   if embed_type == "maum":
#     return MaumEmebedding(cfg, model)
#   elif embed_type == "openai":
#     return OpenAIEmbeddings(
#       model=model,
#       openai_api_key=cfg.embedding.openai_api_key,
#     ) # input args for later use
#   # elif embed_type == "azure":
#   #   return AzureOpenAIEmbeddings()