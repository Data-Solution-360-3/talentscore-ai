"""
Retention purge (Batch 5) — the daily job behind the privacy page's 12-month row
for "parsed CV profile and contact details".

WHY A SCRIPT AND NOT A TTL
    Mongo TTL indexes delete whole documents. The parsed CV and the candidate's
    contact details live as FIELDS inside the screening document, whose scores
    and outcome the employer keeps — so enforcement here is a field-level $unset
    on screenings older than 12 months, stamped with retention_purged_at.
    Transcripts, written answers, and email bodies are whole-document records
    and are handled by TTL indexes in database.connect() instead.

DATE MATH
    Cutoff = created_at < (now − 365 days): 12 months from record CREATION.
    Idempotent — already-purged docs (retention_purged_at set) are skipped.

RUN
    Daily via tc-retention.timer (systemd, like tc-backup). Manual run is safe.
"""

import os
import sys
from datetime import datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from dotenv import load_dotenv
load_dotenv()

from pymongo import MongoClient

PURGE_AFTER_DAYS = 365
PURGE_FIELDS = ["parsed_cv", "applicant_email", "applicant_phone"]


def main():
    db = MongoClient(os.environ["MONGO_URI"], serverSelectionTimeoutMS=20000,
                     tlsAllowInvalidCertificates=True)[os.getenv("DB_NAME", "talentscore")]
    cutoff = datetime.utcnow() - timedelta(days=PURGE_AFTER_DAYS)
    r = db.screenings.update_many(
        {"created_at": {"$lt": cutoff},
         "retention_purged_at": {"$exists": False},
         "$or": [{f: {"$exists": True}} for f in PURGE_FIELDS]},
        {"$unset": {f: "" for f in PURGE_FIELDS},
         "$set": {"retention_purged_at": datetime.utcnow()}})
    print(f"{datetime.utcnow().isoformat()}Z retention purge: cutoff={cutoff.date()} "
          f"screenings purged={r.modified_count}")


if __name__ == "__main__":
    main()
