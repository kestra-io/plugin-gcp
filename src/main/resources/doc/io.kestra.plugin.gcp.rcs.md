# How to use the Google Cloud RCS plugin

Google Cloud Rich Communication Services (RCS) plugin for Business Messaging enables Kestra workflows to send Rich Communication Services (RCS) messages via Google RCS Business Messaging (RBM).

This sub-package provides tasks to send rich interactive messages — plain text, Rich Cards with images and action buttons, carousels, and file attachments — to end users directly from orchestrated workflows.

## Authentication

All tasks inherit GCP authentication properties from the common GCP plugin structure. You can authenticate using:
- A service account JSON key via the `serviceAccount` property.
- Environment-provided credentials (e.g. `GOOGLE_APPLICATION_CREDENTIALS` environment variable).
- Metadata-service-provided IAM credentials when running inside Google Cloud (GKE, GCE, etc.).

Authentication requires `roles/rcsbusinessmessaging.admin` configured on the Brand Agent.

## Tasks

### SendMessage
Sends a plain text RCS message to a recipient's phone number (MSISDN).
- `text`: The plain text content of the message.

### SendRichCard
Sends a standalone Rich Card to a recipient's phone number (MSISDN).
- `title`: The card title.
- `description`: The card description.
- `imageUrl`: The card media/image URL.
- `suggestions`: The list of suggested action/reply buttons (max 4).

### SendCarousel
Sends a carousel of 2–10 Rich Cards; useful for product lists, menu options, or step-by-step instructions.
- `cards`: The list of rich cards in the carousel (between 2 and 10 cards).

### SendFile
Sends a file attachment (image, video, audio, or PDF) to a recipient's phone number.
- `file`: The file attachment URI (can be a public HTTPS URL or a Kestra internal storage URI).
- `thumbnail`: Optional thumbnail image URI.
