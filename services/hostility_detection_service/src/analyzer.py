from typing import List, Dict, Any
from shared.utils.logger import logger


class Analyzer:
    def _analyze_content(
        self,
        text: str,
        doc_id: str,
        problem_pairs: List[tuple[str, str]],
        danger_threshold: float,
        moderate_words: List[Any],
        hostile_words: List[Any],
    ) -> Dict[str, Any]:
        """Analyze content to evaluate the 'danger' of the document"""

        text_lower = text.lower()
        total_words = len(text.split())

        # Hostile words (double weight)
        hostile_found = self._find_words_in_text(text_lower, hostile_words)
        hostile_score = len(hostile_found) * 2

        # Moderate words (weight 1)
        moderate_found = self._find_words_in_text(text_lower, moderate_words)
        moderate_score = len(moderate_found)

        # Problematic pairs (double weight)
        pairs_found = self._find_problem_pairs(problem_pairs, text_lower)
        pairs_score = len(pairs_found) * 2

        # Total danger score
        total_danger_score = hostile_score + moderate_score + pairs_score

        # === Hybrid Calculation ===
        # 1. Relative danger (danger percent vs text length)
        if total_words > 0:
            relative_percent = (total_danger_score / total_words) * 100
        else:
            relative_percent = 0.0

        # 2. Absolute danger (based only on suspicious items)
        absolute_count = len(hostile_found) + len(moderate_found) + len(pairs_found)

        # Decide final percent: weighted average (70% relative, 30% absolute normalization)
        if absolute_count > 0:
            absolute_percent = (absolute_count / (absolute_count + 5)) * 100
            danger_percent = (0.7 * relative_percent) + (0.3 * absolute_percent)
        else:
            danger_percent = relative_percent

        # === Decision logic ===
        is_dangerous = (
            danger_percent >= danger_threshold
            or len(hostile_found) >= 3
            or len(pairs_found) >= 2
        )

        threat_level = self._calculate_threat_level(
            danger_percent, danger_threshold, len(hostile_found), len(pairs_found)
        )

        result = {
            "document_id": doc_id,
            "bds_percent": round(danger_percent, 2),
            "is_bds": is_dangerous,
            "bds_threat_level": threat_level,
        }

        logger.info(
            f"Analysis completed - {doc_id}: dangerous={is_dangerous} "
            f"({danger_percent:.1f}%), level={threat_level}"
        )
        return result

    def _find_words_in_text(self, text: str, word_list: List[str]) -> List[str]:
        return [word for word in word_list if word in text]

    def _find_problem_pairs(
        self, problem_pairs: List[tuple[str, str]], text: str
    ) -> List[str]:
        found_pairs = []
        for word1, word2 in problem_pairs:
            if word1 in text and word2 in text:
                found_pairs.append(f"{word1}+{word2}")
        return found_pairs

    def _calculate_threat_level(
        self, danger_percent: float, danger_threshold: float, hostile_count: int, pairs_count: int
    ) -> str:
        if not (danger_percent >= danger_threshold or hostile_count or pairs_count):
            return "none"
        if hostile_count >= 3 or pairs_count >= 2 or danger_percent >= 20:
            return "high"
        return "medium"
