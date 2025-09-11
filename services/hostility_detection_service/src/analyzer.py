from typing import List, Dict, Any
import time
from shared.utils.logger import logger


class Analyzer:

    def _analyze_content(self, text: str, doc_id: str
                         ,problem_pairs: List[tuple[str,str]]
                         ,danger_threshold: float
                         ,moderate_words: List[Any]
                         ,hostile_words: List[Any] ) -> Dict[str, Any]:
        """Analyze content to evaluate the "danger" of the document"""

        text_lower = text.lower()

        #  Count hostile words (double weight)
        hostile_found = self._find_words_in_text(text_lower,hostile_words)
        hostile_score = len(hostile_found) * 2  # Double weight for hostile_words

        #  Count moderate words (regular weight)
        moderate_found = self._find_words_in_text(text_lower, moderate_words)
        moderate_score = len(moderate_found) * 1

        #  Check for problematic word pairs
        pairs_found = self._find_problem_pairs(problem_pairs,text_lower)
        pairs_score = len(pairs_found) * 1

        #  Calculate total words in text
        total_words = len(text.split())

        # Calculate danger percentage (bds_percent field)
        total_danger_score = hostile_score + moderate_score + pairs_score
        if total_words > 0:
            danger_percent = (total_danger_score / total_words) * 100
        else:
            danger_percent = 0.0

        # Determine if message is flagged (is_bds field)
        is_dangerous = danger_percent >= danger_threshold

        #  Determine threat level (bds_threat_level field)
        threat_level = self._calculate_threat_level(danger_percent,danger_threshold, len(hostile_found))

        # Create final result
        result = {
            "document_id": doc_id,  # include document reference for later storage
            # Required fields from document
            "bds_percent": round(danger_percent, 2),
            "is_bds": is_dangerous,
            "bds_threat_level": threat_level,
        }

        logger.info(
            f"Analysis completed - {doc_id}: dangerous={is_dangerous} ({danger_percent:.1f}%), level={threat_level}")

        return result

    def _find_words_in_text(self, text: str, word_list: List[str]) -> List[str]:
        """Find words from the list in text and return found words as list"""
        found_words = []
        for word in word_list:
            if word in text:
                found_words.append(word)

        return found_words

    def _find_problem_pairs(self,problem_pairs:List[tuple[str,str]], text: str) -> List[str]:
        """Check if there are problematic word pairs in text"""
        found_pairs = []
        for word1, word2 in problem_pairs:
            # If both words appear in text
            if word1 in text and word2 in text:
                found_pairs.append(f"{word1}+{word2}")

        return found_pairs

    def _calculate_threat_level(self,danger_threshold, danger_percent: float, hostile_count: int) -> str:
        """Calculate threat level according to : none, medium, high"""

        # High threat - many hostile words or high percentage
        if hostile_count >= 3 or danger_percent >= 25:
            return "high"

        # Medium threat  above threshold or has hostile words
        elif danger_percent >= danger_threshold or hostile_count >= 1:
            return "medium"

        # No threat
        else:
            return "none"

