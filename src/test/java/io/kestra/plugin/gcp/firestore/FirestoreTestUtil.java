package io.kestra.plugin.gcp.firestore;

import com.google.cloud.firestore.Firestore;

import static io.kestra.core.utils.Rethrow.throwConsumer;

final class FirestoreTestUtil {
    private FirestoreTestUtil() {
        // utility class pattern: prevent initialization
    }

    public static void clearCollection(Firestore firestore, String collectionName) throws Exception {
        var collection = firestore.collection(collectionName);
        collection.listDocuments().forEach(throwConsumer(doc -> doc.delete().get()));
    }
}
