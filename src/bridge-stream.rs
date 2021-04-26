pub struct DropReceiver<T> {
    chan: oneshot::Sender<usize>,
    inner: mpsc::Receiver<T>,
}

impl<T> tokio::stream::Stream for DropReceiver<T> {
    type Item = T;

    fn poll_next(mut self: std::pin::Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<T>> {
        self.inner.poll_recv(cx)
    }
}

impl<T> Deref for DropReceiver<T> {
    type Target = mpsc::Receiver<T>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<T> Drop for DropReceiver<T> {
    fn drop(&mut self) {
        debug!("Receiver has been dropped");
        self.chan.send(1).await;
    }
}
