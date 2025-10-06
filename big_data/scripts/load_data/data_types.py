"""Data types used for parsing and loading data for project."""

from __future__ import annotations

import json
from typing import NamedTuple, Self


class YouTubeData(NamedTuple):
    """Tuple that describes YouTube video data."""

    id: str
    uploader_un: str
    age_days: int
    category: str
    length_s: int
    views: int
    video_rate: float
    num_ratings: int
    num_comments: int
    related_ids: list[str]

    @classmethod
    def from_csv(cls, csv_line: list[str]) -> Self:
        """Create YouTube data from list of csv fields."""
        match csv_line:
            case [id, un, age, cat, l, vs, rt, nr, nc, *r_ids]:
                return cls(
                    id,
                    un,
                    int(age),
                    cat,
                    int(l),
                    int(vs),
                    float(rt),
                    int(nr),
                    int(nc),
                    r_ids,
                )
            case _:
                msg = f"unexpected fields found from line {csv_line}"
                raise TypeError(msg)

    def to_json(self, name: str) -> str:
        """Convert to json string for storage in another file."""
        d = {
            "id": self.id,
            "date_collected": name,
            "desc": {
                "uploader": self.uploader_un,
                "age": self.age_days,
                "category": self.category,
            },
            "attr": {
                "length": self.length_s,
                "rate": self.video_rate,
            },
            "engagement": {
                "views": self.views,
                "ratings": self.num_ratings,
                "comments": self.num_comments,
            },
            "r_ids": self.related_ids,
        }
        return json.dumps(d)
