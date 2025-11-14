"""
Google Gemini AI integration for document parsing.
"""

from __future__ import annotations

import json
import logging
import os
import tempfile
from typing import Any, Dict, Optional

import google.generativeai as genai

logger = logging.getLogger(__name__)


class GeminiParser:
    """Thin wrapper around the Google Gemini SDK."""
    
    def __init__(self, api_key: str, model: str = "gemini-2.5-flash"):
        if not api_key:
            raise ValueError("GEMINI_API_KEY must be configured before using GeminiParser.")

        self.api_key = api_key
        self.model_name = model

        try:
            genai.configure(api_key=api_key)
            
            # Configure generation settings for optimal performance
            generation_config = {
                "temperature": 0.1,  # Low temperature for consistent JSON output
                "top_p": 0.95,
                "top_k": 40,
                "max_output_tokens": 16384,  # Increased for large medical reports
            }
            
            # Configure safety settings to avoid blocking medical content
            safety_settings = [
                {"category": "HARM_CATEGORY_HARASSMENT", "threshold": "BLOCK_NONE"},
                {"category": "HARM_CATEGORY_HATE_SPEECH", "threshold": "BLOCK_NONE"},
                {"category": "HARM_CATEGORY_SEXUALLY_EXPLICIT", "threshold": "BLOCK_NONE"},
                {"category": "HARM_CATEGORY_DANGEROUS_CONTENT", "threshold": "BLOCK_NONE"},
            ]
            
            self.model = genai.GenerativeModel(
                model,
                generation_config=generation_config,
                safety_settings=safety_settings
            )
            logger.info("Initialized Gemini model '%s' with optimized config", model)
        except Exception as exc:
            logger.exception("Failed to initialize Gemini model '%s'.", model)
            raise

    def parse_document(self, text_content: str, images: Optional[list] = None) -> Optional[Dict[str, Any]]:
        """
        Parse structured data from document text (with optional images).

        Args:
            text_content: Raw text content extracted from a document.
            images: Optional list of image payloads accepted by Gemini.

        Returns:
            Parsed data as a dictionary, or None if parsing fails.
        """
        try:
            content_parts = [self._get_parsing_prompt()]
            if text_content:
                content_parts.append(f"DOCUMENT TEXT:\n{text_content}")
            if images:
                content_parts.extend(images[:5])

            response = self.model.generate_content(content_parts)
            if not response or not hasattr(response, "text"):
                logger.error("Gemini returned an empty response while parsing document.")
                return None

            return self._extract_json(response.text)
        except Exception as exc:
            logger.exception("Gemini failed to parse document content.")
            raise RuntimeError("Gemini parsing failed") from exc

    def parse_pdf(self, pdf_bytes: bytes, filename: str = "document.pdf") -> Optional[Dict[str, Any]]:
        """
        Parse PDF document using Gemini's native PDF support.

        Args:
            pdf_bytes: Raw PDF bytes.
            filename: Optional filename used for logging.
        """
        import time
        tmp_path = None
        try:
            # Time: File upload to Gemini
            upload_start = time.time()
            with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as tmp_file:
                tmp_file.write(pdf_bytes)
                tmp_path = tmp_file.name

            uploaded_file = genai.upload_file(tmp_path, mime_type="application/pdf")
            upload_time = time.time() - upload_start
            print(f"\n🔵 [{self.model_name}] Upload time: {upload_time:.2f}s for '{filename}'")
            logger.warning("⏱️  Upload time for '%s': %.2f seconds", filename, upload_time)
            
            # Time: Gemini API call
            api_start = time.time()
            
            # Add request options for better performance
            request_options = {
                "timeout": 120,  # 2 minute timeout
            }
            
            response = self.model.generate_content(
                [self._get_parsing_prompt(), uploaded_file],
                request_options=request_options
            )
            api_time = time.time() - api_start
            print(f"\n🔴 [{self.model_name}] API processing time: {api_time:.2f}s for '{filename}'")
            print(f"\n✅ [{self.model_name}] TOTAL TIME: {upload_time + api_time:.2f}s\n")
            logger.warning("⏱️  API processing time for '%s' (model=%s): %.2f seconds", 
                       filename, self.model_name, api_time)

            # Check for safety blocks or empty responses
            if not response:
                logger.error("Gemini returned None response for PDF '%s'.", filename)
                return None
            
            # Check if response was blocked by safety filters
            if response.candidates and response.candidates[0].finish_reason not in [1, 0]:  # 0=UNSPECIFIED, 1=STOP (normal)
                finish_reason = response.candidates[0].finish_reason
                logger.error("Gemini response blocked for PDF '%s'. Finish reason: %s", filename, finish_reason)
                # Try to get partial text if available
                try:
                    if response.candidates[0].content.parts:
                        partial_text = response.candidates[0].content.parts[0].text
                        logger.info("Attempting to extract JSON from partial response...")
                        return self._extract_json(partial_text)
                except Exception:
                    pass
                return None
            
            # Normal response
            if not hasattr(response, "text") or not response.text:
                logger.error("Gemini returned empty text for PDF '%s'.", filename)
                return None

            parsed_json = self._extract_json(response.text)
            if not parsed_json:
                return None

            # Attach usage + cost (metadata preferred, fallback to file-size estimate)
            try:
                inp, out = self._extract_token_usage(response)
                if inp == 0 and out == 0 and tmp_path and os.path.exists(tmp_path):
                    # heuristic estimate: ~4 tokens per KB split 30/70 in/out
                    file_kb = os.path.getsize(tmp_path) / 1024.0
                    est_tokens = max(1, int(file_kb * 4))
                    inp = int(est_tokens * 0.3)
                    out = est_tokens - inp
                cost = self._compute_cost(inp, out)
                parsed_json["usage"] = {
                    "model": self.model_name,
                    "input_tokens": int(inp),
                    "output_tokens": int(out),
                    "cost_usd": float(cost),
                }
            except Exception:
                # don't fail parsing if usage extraction fails
                logger.debug("Failed to extract usage/cost for '%s'", filename, exc_info=True)

            return parsed_json
        except Exception as exc:
            logger.exception("Gemini failed to parse PDF '%s'.", filename)
            raise RuntimeError("Gemini PDF parsing failed") from exc
        finally:
            if tmp_path and os.path.exists(tmp_path):
                try:
                    os.unlink(tmp_path)
                except OSError:
                    logger.debug("Failed to delete temporary file '%s'.", tmp_path, exc_info=True)

    def parse_multiple_pdfs(self, pdf_files: list) -> Optional[Dict[str, Any]]:
        """
        Parse multiple PDFs together to enable accurate cross-document photo comparison.
        
        This method uploads all PDFs to Gemini at once, allowing it to see all images
        from all documents simultaneously and perform accurate photo comparison across files.
        
        Args:
            pdf_files: List of dicts with 'bytes' and 'filename' keys
            
        Returns:
            Consolidated parsed data with accurate cross-document photo comparison
        """
        tmp_paths = []
        try:
            uploaded_files = []
            filenames = []
            
            # Upload all PDFs to Gemini
            for pdf_file in pdf_files:
                with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as tmp_file:
                    tmp_file.write(pdf_file['bytes'])
                    tmp_paths.append(tmp_file.name)
                
                uploaded = genai.upload_file(tmp_file.name, mime_type="application/pdf")
                uploaded_files.append(uploaded)
                filenames.append(pdf_file['filename'])
                logger.info("Uploaded '%s' to Gemini for batch processing", pdf_file['filename'])
            
            # Create enhanced prompt with file listing
            file_list_text = "\n".join([f"  {i+1}. {fname}" for i, fname in enumerate(filenames)])
            batch_prompt = (
                f"🚨 ATTENTION: You are analyzing {len(pdf_files)} DIFFERENT PDF FILES:\n"
                f"{file_list_text}\n\n"
                f"CRITICAL INSTRUCTIONS FOR PHOTO COMPARISON:\n"
                f"- These are SEPARATE documents from DIFFERENT people\n"
                f"- When you find images of people, use the ACTUAL PDF FILENAME in 'source_file' field\n"
                f"- You MUST compare images from DIFFERENT PDF files (person from file 1 vs person from file 2)\n"
                f"- DO NOT compare multiple images from the same PDF file\n"
                f"- Example: Compare person from '{filenames[0]}' with person from '{filenames[1]}'\n\n"
                f"{self._get_parsing_prompt()}"
            )
            
            # Send all PDFs together in one API call
            response = self.model.generate_content([batch_prompt] + uploaded_files)
            
            if not response or not hasattr(response, "text"):
                logger.error("Gemini returned empty response for batch PDF parsing")
                return None
            
            parsed_data = self._extract_json(response.text)
            if not parsed_data:
                return None

            # Attach usage + cost for batch results (metadata-first, fallback to size)
            try:
                inp, out = self._extract_token_usage(response)
                if inp == 0 and out == 0 and tmp_paths:
                    total_kb = sum(os.path.getsize(p) for p in tmp_paths) / 1024.0
                    est_tokens = max(1, int(total_kb * 4))
                    inp = int(est_tokens * 0.3)
                    out = est_tokens - inp
                cost = self._compute_cost(inp, out)
                if isinstance(parsed_data, dict):
                    parsed_data["usage"] = {
                        "model": self.model_name,
                        "input_tokens": int(inp),
                        "output_tokens": int(out),
                        "cost_usd": float(cost),
                    }
                else:
                    parsed_data = {
                        "data": parsed_data,
                        "usage": {
                            "model": self.model_name,
                            "input_tokens": int(inp),
                            "output_tokens": int(out),
                            "cost_usd": float(cost),
                        },
                    }
            except Exception:
                logger.debug("Failed to extract batch usage/cost", exc_info=True)

            return parsed_data
            
        except Exception as exc:
            logger.exception("Failed to parse multiple PDFs together")
            raise RuntimeError("Batch PDF parsing failed") from exc
        finally:
            # Clean up temporary files
            for tmp_path in tmp_paths:
                if os.path.exists(tmp_path):
                    try:
                        os.unlink(tmp_path)
                    except OSError:
                        logger.debug("Failed to delete temp file '%s'", tmp_path, exc_info=True)

    @staticmethod
    def _extract_json(response_text: str) -> Optional[Dict[str, Any]]:
        """
        Attempt to extract JSON payload from Gemini response.

        Gemini often wraps JSON in markdown code fences; this method tolerates that.
        """
        if not response_text:
            return None

        cleaned = response_text.strip()
        if cleaned.startswith("```"):
            cleaned = cleaned.strip("`")
            # Remove optional language identifier (e.g. ```json)
            newline_index = cleaned.find("\n")
            if newline_index != -1:
                cleaned = cleaned[newline_index + 1 :]

        try:
            return json.loads(cleaned)
        except json.JSONDecodeError as e:
            logger.error("JSON parsing failed at position %d: %s", e.pos, str(e))
            logger.error("Response text (first 1000 chars): %s", cleaned[:1000])
            
            # Try to salvage incomplete JSON by finding the last complete object
            try:
                # Find the last complete JSON object by searching for the last '}' 
                last_brace = cleaned.rfind('}')
                if last_brace > 0:
                    truncated = cleaned[:last_brace + 1]
                    logger.info("Attempting to parse truncated JSON (up to position %d)", last_brace)
                    return json.loads(truncated)
            except Exception:
                pass
            
            return None

    @staticmethod
    def _get_parsing_prompt() -> str:
        """Prompt that instructs Gemini how to extract medical information."""
        return (
            "You are a medical report extraction service. Return concise JSON with fields such as "
            "patient_name, patient_id, encounter_date, diagnosis, medications, procedures, "
            "lab_results, imaging_findings, recommendations, and clinician_name. "
            "\n\n"
            "UNIVERSAL TEST RESULTS FORMAT:\n"
            "For ALL test results, group them by examination/section name with this structure:\n"
            "\"lab_results\": [\n"
            "  {\n"
            "    \"examination_name\": \"Section or test group name (e.g., Complete Blood Count, Urine Analysis, Lipid Profile, etc.)\",\n"
            "    \"tests\": [\n"
            "      {\n"
            "        \"test_name\": \"specific test name\",\n"
            "        \"result\": \"numeric or text value\",\n"
            "        \"unit\": \"measurement unit (mg/dl, mmHg, cm, kg, bpm, etc.)\",\n"
            "        \"reference_range\": \"normal range if provided\",\n"
            "        \"status\": \"Normal/Medium/High/Low\"\n"
            "      }\n"
            "    ]\n"
            "  }\n"
            "]\n"
            "\n"
            "If examination names are not explicitly grouped in the document, create logical groups like:\n"
            "- 'Complete Blood Count' for hemoglobin, WBC, RBC, platelets\n"
            "- 'Lipid Profile' for cholesterol, triglycerides, HDL, LDL\n"
            "- 'Liver Function Test' for SGOT, SGPT, bilirubin\n"
            "- 'Kidney Function Test' for creatinine, urea, BUN\n"
            "- 'Urine Analysis' for urine tests\n"
            "- 'Vitals' for BP, pulse, temperature, height, weight\n"
            "\n"
            "CRITICAL INSTRUCTIONS - APPLY TO ALL TESTS:\n"
            "- Extract unit separately for EVERY measurement (Height: 172 cm → result='172', unit='cm')\n"
            "- Extract status for EVERY test by comparing with reference range:\n"
            "  * 'Normal' = within reference range or explicitly marked as normal\n"
            "  * 'High' = above reference range or marked as High/Elevated/Abnormal\n"
            "  * 'Medium' = slightly outside normal range but not critical\n"
            "  * 'Low' = below reference range or marked as Low/Decreased\n"
            "- This applies to:\n"
            "  * Lab results (blood tests, urine tests, etc.)\n"
            "  * Vitals (BP, pulse, temperature, height, weight, BMI)\n"
            "  * Imaging results (X-ray, CT, MRI findings)\n"
            "  * ECG/EEG results\n"
            "  * Any other measurements or test results\n"
            "- If no unit exists, set unit to null or empty string\n"
            "- If no reference range or status cannot be determined, set status to null\n"
            "- Use document's exact status terminology if explicitly stated\n"
            "\n"
            "CONFIDENCE REQUIREMENTS:\n"
            "Always include these fields at the very top level of the JSON response:\n"
            "- confidence_score: integer 0-100 representing your confidence that the extracted data is correct\n"
            "- confidence_summary: short sentence explaining why you chose that score (mention document clarity, legibility, and any uncertainties)\n"
            "Score guidance: 90-100 = crystal clear typed PDF, 70-89 = mostly clear with minor issues, 40-69 = several uncertain sections, below 40 = document barely legible.\n"
            "\n"
            "PHOTO COMPARISON FEATURE:\n"
            "If the document(s) contain images of a human face (such as patient photos, ID photos, passport photos, or before/after images), "
            "you MUST analyze and compare these images. Include a 'photo_comparison' field in your JSON response with:\n"
            "{\n"
            "  \"images_found\": [\n"
            "    {\"image_number\": 1, \"description\": \"description of person\", \"source_file\": \"actual_filename.pdf\"},\n"
            "    {\"image_number\": 2, \"description\": \"description of person\", \"source_file\": \"actual_filename.pdf\"}\n"
            "  ],\n"
            "  \"comparison_performed\": true,\n"
            "  \"match\": \"YES\" or \"NO\",\n"
            "  \"reason\": \"detailed explanation comparing facial features\",\n"
            "  \"confidence\": \"high/medium/low\"\n"
            "}\n"
            "\n"
            "CRITICAL INSTRUCTIONS:\n"
            "1. ALWAYS perform comparison if 2+ human face images are found (even in the same PDF)\n"
            "2. List ALL human images found with their descriptions\n"
            "3. Use actual PDF filename in source_file field\n"
            "4. Compare ALL faces found - describe specific facial features (eyes color/shape, nose, mouth, jawline, facial structure, age, gender)\n"
            "5. Match = \"YES\" ONLY if all images show the SAME EXACT PERSON\n"
            "6. Match = \"NO\" if images show DIFFERENT PEOPLE (even if similar age/gender/ethnicity)\n"
            "7. If only 1 human image found, set comparison_performed=false and explain why\n"
            "8. If NO human images found, set photo_comparison to null\n"
            "\n"
            "NEVER skip photo comparison if human faces are present. This is a critical feature.\n"
            "\n"
            "MEDICAL HISTORY QUESTIONNAIRE EXTRACTION:\n"
            "Many medical reports contain a questionnaire section with YES/NO checkboxes. Look for sections titled:\n"
            "- 'Medical History Questions'\n"
            "- 'Health Questionnaire'\n"
            "- 'Medical Examination Questions'\n"
            "- Tables with questions in one column and YES/NO checkboxes in other columns\n"
            "\n"
            "When you find such questionnaires:\n"
            "1. Extract ALL questions exactly as written\n"
            "2. Identify which box is checked (YES or NO) - look for checkmarks, ticks, crosses, or circled options\n"
            "3. Include a 'medical_history_questions' array in your response:\n"
            "[\n"
            "  {\n"
            "    \"question\": \"exact question text from document\",\n"
            "    \"answer\": \"YES\" or \"NO\" (based on which checkbox is marked)\n"
            "  }\n"
            "]\n"
            "\n"
            "IMPORTANT for checkbox detection:\n"
            "- Look carefully at the YES and NO columns\n"
            "- A checkmark (✓), tick, circle, or any marking indicates that option is selected\n"
            "- If the NO column has a marking, the answer is \"NO\"\n"
            "- If the YES column has a marking, the answer is \"YES\"\n"
            "- If both or neither are marked, note \"UNCLEAR\" as the answer\n"
            "\n"
            "Only include fields that are present in the source document."
        )
    # ---- Added: token extraction + cost helper (metadata-first, file-size fallback) ----
    @staticmethod
    def _extract_token_usage(response) -> tuple[int, int]:
        """Try common shapes in SDK raw response to extract input/output token counts."""
        try:
            raw = getattr(response, "_raw_response", None) or getattr(response, "usage_metadata", None)
            # dict-like raw
            if isinstance(raw, dict):
                usage = raw.get("usage") or raw.get("usage_metadata") or raw
                if isinstance(usage, dict):
                    inp = int(usage.get("prompt_tokens") or usage.get("input_tokens") or 0)
                    out = int(usage.get("completion_tokens") or usage.get("output_tokens") or 0)
                    return inp, out
            # attribute-style raw
            if raw:
                inp = int(getattr(raw, "prompt_token_count", 0) or 0)
                out = int(getattr(raw, "candidates_token_count", 0) or 0)
                return inp, out
        except Exception:
            pass
        return 0, 0

    @staticmethod
    def _compute_cost(input_tokens: int, output_tokens: int) -> float:
        """Compute USD cost using your requested formula."""
        input_cost = (input_tokens / 1_000_000) * 0.30
        output_cost = (output_tokens / 1_000_000) * 2.50
        return round(input_cost + output_cost, 6)
    # ---- end helpers ----
