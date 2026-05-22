import os
from dotenv import load_dotenv
from src.llm.vector_memory import CypherVectorMemory

load_dotenv()

openai_api_key = os.getenv("OPENAI_API_KEY")
if not openai_api_key:
    raise ValueError("OPENAI_API_KEY is not set. Add it to your .env file.")

mem = CypherVectorMemory(openai_api_key=openai_api_key)
count = mem.populate(force=True)
print(f"Indexed {count} examples into ChromaDB at data/cypher_vector_db/")