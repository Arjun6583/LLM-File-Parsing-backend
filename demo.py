import google.generativeai as genai

# Configure the API key
genai.configure(api_key="AIzaSyA-nIR5wXp2SZJoka7UafQvn1c5rmjEmhU")

# Initialize the model
model = genai.GenerativeModel(model_name="gemini-2.0-flash-exp")

# Define your prompt
prompt = "Tell me about virat kohli and rohit sharama performance in cricket"
# Generate content with configuration
response = model.generate_content(
    prompt,
    generation_config={
        "temperature": 0.1,
        "max_output_tokens": 2000,
    }
)

# Extract and print the output
print(response.text)
