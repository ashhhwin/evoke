from gradio_app_new import app as gradio_app

if __name__ == "__main__":
    gradio_app.launch(
        server_name="0.0.0.0",
        server_port=7869,
        share=False,
        debug=False
    )
