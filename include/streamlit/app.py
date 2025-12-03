import streamlit as st
from openai import OpenAI
import weaviate
import os

WEAVIATE_CLASS_NAME = os.getenv("WEAVIATE_CLASS_NAME")


def get_relevant_chunks(user_prompt, limit=5, certainty=0.75):
    import weaviate.classes as wvc

    client = weaviate.connect_to_local(
        host="weaviate",
        port=8081,
        auth_credentials=weaviate.auth.AuthApiKey("adminkey"),
        headers={"X-OpenAI-Api-key": os.getenv("OPENAI_API_KEY")},
        skip_init_checks=True,
    )

    try:
        source = client.collections.get(WEAVIATE_CLASS_NAME)

        response = source.query.near_text(
            query=user_prompt,
            distance=certainty,
            return_metadata=wvc.query.MetadataQuery(distance=True),
            limit=limit,
        )

    finally:
        client.close()

    return response.objects


def get_response(chunks, user_prompt):
    inference_prompt = """You are the helpful social post generator Astra! You will create an interesting factoid post 
    about Apache Airflow and the topic requested by the user:"""

    for chunk in chunks:
        chunk = chunk.properties
        chunk_title = chunk.get("title", "unknown")
        chunk_folder = chunk.get("folder_path", "unknown")
        chunk_text = chunk.get("text", "no text")

        chunk_info = f"Source: {chunk_folder} - {chunk_title}. Text: {chunk_text}"
        inference_prompt += " " + chunk_info + " "

    inference_prompt += "Your user asks:"

    inference_prompt += " " + user_prompt

    inference_prompt += """ 
    Remember to keep the post short and sweet! At the end of the post add another sentence that is a space fact!"""

    client = OpenAI()

    champion_model_id = "gpt-4"

    chat_completion = client.chat.completions.create(
        model=champion_model_id,
        messages=[{"role": "user", "content": inference_prompt}],
    )

    return chat_completion


# ------------------ #
# STREAMLIT APP CODE #
# ------------------ #

st.title("My own use case!")

st.header("Search")

user_prompt = st.text_input(
    "Your post idea:",
    "Create a LinkedIn post for me about dynamic task mapping!",
)
limit = st.slider("Retrieve X most relevant chunks:", 1, 20, 5)
certainty = st.slider("Certainty threshold for relevancy", 0.0, 1.0, 0.75)

if st.button("Generate post!"):
    st.header("Answer")
    with st.spinner(text="Thinking... :thinking_face:"):
        chunks = get_relevant_chunks(user_prompt, limit=limit, certainty=certainty)
        response = get_response(chunks=chunks, user_prompt=user_prompt)

        st.success("Done! :smile:")

        st.write(response.choices[0].message.content)

        st.header("Sources")

        for chunk in chunks:
            props = chunk.properties
            st.write(f"Folder: {props.get('folder_path', 'N/A')}")
            st.write(f"Title: {props.get('title', 'N/A')}")
            if hasattr(chunk.metadata, 'distance'):
                st.write(f"Distance: {chunk.metadata.distance:.4f}")
            st.write("---")