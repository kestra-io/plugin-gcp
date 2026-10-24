package io.kestra.plugin.gcp.firestore;

import java.util.concurrent.TimeUnit;

import com.google.cloud.firestore.Firestore;

abstract class FirestoreTestUtil {
    public static void clearCollection(Firestore firestore, String collectionName) throws Exception {
        var documents = firestore.collection(collectionName)
            .get()
            .get(30, TimeUnit.SECONDS)
            .getDocuments();

        for (var document : documents) {
            document.getReference().delete().get(30, TimeUnit.SECONDS);
        }
    }
}
