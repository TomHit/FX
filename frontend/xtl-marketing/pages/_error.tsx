function Error({ statusCode }: { statusCode?: number }) {
  return (
    <main style={{ padding: 24 }}>
      <h1>{statusCode ? `Error ${statusCode}` : "An error occurred"}</h1>
    </main>
  );
}

Error.getInitialProps = ({ res, err }: any) => {
  const statusCode = res ? res.statusCode : err ? err.statusCode : 404;
  return { statusCode };
};

export default Error;
