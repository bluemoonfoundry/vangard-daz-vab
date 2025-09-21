import logging
from typing import Dict, List, Optional

import torch
from transformers import AutoModelForCausalLM, AutoTokenizer, GenerationConfig


class GemmaLocalChat:
    """
    A local chat interface using Google's Gemma-1.1-7B-IT model via Hugging Face transformers.
    """

    def __init__(
        self,
        model_name: str = "google/gemma-1.1-7b-it",
        device: str = "auto",
        torch_dtype: torch.dtype = torch.float16,
        trust_remote_code: bool = True,
        max_memory: Optional[Dict] = None,
    ):
        """
        Initialize the Gemma chat model.

        Args:
            model_name: HuggingFace model identifier
            device: Device to load model on ('auto', 'cuda', 'cpu')
            torch_dtype: Data type for model weights
            trust_remote_code: Whether to trust remote code
            max_memory: Memory constraints for model loading
        """
        self.model_name = model_name
        self.device = self._setup_device(device)
        self.torch_dtype = torch_dtype

        # Initialize tokenizer and model
        self.tokenizer = None
        self.model = None

        # Chat history
        self.chat_history: List[Dict[str, str]] = []

        # Default generation config
        self.generation_config = GenerationConfig(
            max_new_tokens=512,
            temperature=0.7,
            top_p=0.9,
            do_sample=True,
            pad_token_id=None,  # Will be set after tokenizer load
            eos_token_id=None,  # Will be set after tokenizer load
        )

        # Load model and tokenizer
        self._load_model(trust_remote_code, max_memory)

    def _setup_device(self, device: str) -> torch.device:
        """Setup the appropriate device for model inference."""
        if device == "auto":
            if torch.cuda.is_available():
                return torch.device("cuda")
            else:
                return torch.device("cpu")
        return torch.device(device)

    def _load_model(self, trust_remote_code: bool, max_memory: Optional[Dict]):
        """Load the tokenizer and model."""
        try:
            print(f"Loading tokenizer from {self.model_name}...")
            self.tokenizer = AutoTokenizer.from_pretrained(
                self.model_name, trust_remote_code=trust_remote_code
            )

            # Set pad token if not present
            if self.tokenizer.pad_token is None:
                self.tokenizer.pad_token = self.tokenizer.eos_token

            print(f"Loading model from {self.model_name}...")
            model_kwargs = {
                "torch_dtype": self.torch_dtype,
                "trust_remote_code": trust_remote_code,
                "device_map": "auto" if self.device.type == "cuda" else None,
            }

            if max_memory:
                model_kwargs["max_memory"] = max_memory

            self.model = AutoModelForCausalLM.from_pretrained(
                self.model_name, **model_kwargs
            )

            if self.device.type == "cpu":
                self.model = self.model.to(self.device)

            # Update generation config with tokenizer info
            self.generation_config.pad_token_id = self.tokenizer.pad_token_id
            self.generation_config.eos_token_id = self.tokenizer.eos_token_id

            print(f"Model loaded successfully on {self.device}")

        except Exception as e:
            logging.error(f"Error loading model: {e}")
            raise

    def _format_chat_prompt(
        self, user_message: str, include_history: bool = True
    ) -> str:
        """
        Format the chat prompt for Gemma using the instruction format.

        Args:
            user_message: The user's input message
            include_history: Whether to include chat history in the prompt

        Returns:
            Formatted prompt string
        """
        if include_history and self.chat_history:
            # Build conversation context
            conversation = ""
            for turn in self.chat_history[-5:]:  # Keep last 5 turns for context
                conversation += (
                    f"User: {turn['user']}\nAssistant: {turn['assistant']}\n\n"
                )

            prompt = f"{conversation}User: {user_message}\nAssistant:"
        else:
            # Simple single-turn format
            prompt = f"User: {user_message}\nAssistant:"

        return prompt

    def chat(
        self,
        message: str,
        max_new_tokens: int = 512,
        temperature: float = 0.7,
        top_p: float = 0.9,
        include_history: bool = True,
        save_to_history: bool = True,
    ) -> str:
        """
        Generate a chat response for the given message.

        Args:
            message: User input message
            max_new_tokens: Maximum tokens to generate
            temperature: Sampling temperature
            top_p: Top-p sampling parameter
            include_history: Whether to include chat history
            save_to_history: Whether to save this exchange to history

        Returns:
            Generated response string
        """
        if not self.model or not self.tokenizer:
            raise RuntimeError(
                "Model not loaded. Please initialize the class properly."
            )

        # Format the prompt
        prompt = self._format_chat_prompt(message, include_history)

        # Tokenize input
        inputs = self.tokenizer(
            prompt, return_tensors="pt", padding=True, truncation=True, max_length=2048
        ).to(self.device)

        # Update generation config
        gen_config = GenerationConfig(
            max_new_tokens=max_new_tokens,
            temperature=temperature,
            top_p=top_p,
            do_sample=True,
            pad_token_id=self.tokenizer.pad_token_id,
            eos_token_id=self.tokenizer.eos_token_id,
        )

        # Generate response
        with torch.no_grad():
            outputs = self.model.generate(
                input_ids=inputs.input_ids,
                attention_mask=inputs.attention_mask,
                generation_config=gen_config,
                use_cache=True,
            )

        # Decode response (only the new tokens)
        new_tokens = outputs[0][inputs.input_ids.shape[1] :]
        response = self.tokenizer.decode(new_tokens, skip_special_tokens=True).strip()

        # Clean up response (remove any remaining prompt artifacts)
        if response.startswith("Assistant:"):
            response = response[10:].strip()

        # Save to history if requested
        if save_to_history:
            self.chat_history.append({"user": message, "assistant": response})

        return response

    def clear_history(self):
        """Clear the chat history."""
        self.chat_history.clear()
        print("Chat history cleared.")

    def get_history(self) -> List[Dict[str, str]]:
        """Get the current chat history."""
        return self.chat_history.copy()

    def set_generation_config(self, **kwargs):
        """Update the generation configuration."""
        for key, value in kwargs.items():
            if hasattr(self.generation_config, key):
                setattr(self.generation_config, key, value)
            else:
                print(f"Warning: {key} is not a valid generation config parameter")

    def save_conversation(self, filename: str):
        """Save the current conversation to a file."""
        import json

        try:
            with open(filename, "w", encoding="utf-8") as f:
                json.dump(self.chat_history, f, indent=2, ensure_ascii=False)
            print(f"Conversation saved to {filename}")
        except Exception as e:
            print(f"Error saving conversation: {e}")

    def load_conversation(self, filename: str):
        """Load a conversation from a file."""
        import json

        try:
            with open(filename, "r", encoding="utf-8") as f:
                self.chat_history = json.load(f)
            print(f"Conversation loaded from {filename}")
        except Exception as e:
            print(f"Error loading conversation: {e}")


# Example usage
if __name__ == "__main__":
    # Initialize the chat model
    try:
        chat = GemmaLocalChat()

        print("Gemma Chat initialized! Type 'quit' to exit, 'clear' to clear history.")
        print("-" * 50)

        while True:
            user_input = input("\nYou: ").strip()

            if user_input.lower() in ["quit", "exit", "q"]:
                break
            elif user_input.lower() == "clear":
                chat.clear_history()
                continue
            elif user_input.lower() == "history":
                history = chat.get_history()
                for i, turn in enumerate(history, 1):
                    print(f"{i}. User: {turn['user']}")
                    print(f"   Assistant: {turn['assistant']}")
                continue
            elif not user_input:
                continue

            # Generate response
            try:
                response = chat.chat(user_input)
                print(f"\nAssistant: {response}")
            except Exception as e:
                print(f"Error generating response: {e}")

    except Exception as e:
        print(f"Error initializing chat: {e}")
        print("Make sure you have the required dependencies installed:")
        print("pip install torch transformers accelerate")
