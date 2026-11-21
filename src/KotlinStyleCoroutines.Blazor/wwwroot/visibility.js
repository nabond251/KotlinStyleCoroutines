export function observeVisibility(dotNetRef) {
    const handler = () => {
        dotNetRef.invokeMethodAsync('OnVisibilityChanged', !document.hidden);
    };

    document.addEventListener('visibilitychange', handler);

    return {
        dispose: () => {
            document.removeEventListener('visibilitychange', handler);
        }
    };
}
