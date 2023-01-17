package io.kestra.plugin.gcp.firestore;

import com.google.cloud.firestore.Firestore;

import static io.kestra.core.utils.Rethrow.throwConsumer;

abstract class FirestoreTestUtil {
    public static void clearCollection(Firestore firestore, String collectionName) throws Exception {
        var collection = firestore.collection(collectionName);
        collection.listDocuments().forEach(throwConsumer(doc -> doc.delete().get()));
    }
}
